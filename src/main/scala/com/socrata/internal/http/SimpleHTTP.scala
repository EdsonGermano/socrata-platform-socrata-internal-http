package com.socrata.internal.http

import java.io._
import java.nio.charset.{UnsupportedCharsetException, IllegalCharsetNameException, Charset, StandardCharsets}
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.client.methods._
import org.apache.http.entity._
import com.rojoma.simplearm._
import com.rojoma.simplearm.util._
import com.rojoma.json.io._
import javax.activation.{MimeTypeParseException, MimeType}
import java.lang.reflect.UndeclaredThrowableException
import org.apache.http.entity.mime.MultipartEntity
import org.apache.http.entity.mime.content.InputStreamBody
import org.apache.http.impl.conn.PoolingClientConnectionManager
import org.apache.http.params.{CoreProtocolPNames, HttpProtocolParams, HttpConnectionParams}
import org.apache.http.conn.ConnectTimeoutException
import java.net._
import com.socrata.internal.http.pingpong._
import java.util.concurrent.{TimeUnit, Phaser, CyclicBarrier}
import scala.Some
import scala.Some

trait ResponseInfo {
  def resultCode: Int
  def headers(name: String): Array[String] // this will return an empty array if the header does not exist
  def headerNames: Set[String]
}

trait ResponseInfoProvider {
  def responseInfo: ResponseInfo
}

class HttpClientException extends Exception

class HttpClientTimeoutException extends HttpClientException
class ConnectTimeout extends HttpClientTimeoutException
class ReceiveTimeout extends HttpClientTimeoutException
class FullTimeout extends HttpClientTimeoutException

class LivenessCheckFailed extends HttpClientException
class ConnectFailed extends HttpClientException // failed for not-timeout reasons
class NoBodyInResponse extends HttpClientException

class ContentTypeException extends HttpClientException
class NoContentTypeInResponse extends ContentTypeException
class MultipleContentTypesInResponse extends ContentTypeException
class UnparsableContentType(val contentType: String) extends ContentTypeException
class UnexpectedContentType(val got: String, val expected: String) extends ContentTypeException
class IllegalCharsetName(val charsetName: String) extends ContentTypeException
class UnsupportedCharset(val charsetName: String) extends ContentTypeException

trait HttpClient extends Closeable {
  import HttpClient._

  type RawResponse = InputStream with Acknowledgeable with ResponseInfoProvider
  type RawJsonResponse = Reader with Acknowledgeable with ResponseInfoProvider
  type JsonResponse = Iterator[JsonEvent] with Acknowledgeable with ResponseInfoProvider

  protected def connectTimeout() = throw new ConnectTimeout
  protected def receiveTimeout() = throw new ReceiveTimeout
  protected def connectFailed() = throw new ConnectFailed
  protected def livenessCheckFailed() = throw new LivenessCheckFailed
  protected def fullTimeout() = throw new FullTimeout
  protected def noContentTypeInResponse() = throw new NoContentTypeInResponse
  protected def multipleContentTypesInResponse() = throw new MultipleContentTypesInResponse
  protected def unparsableContentType(contentType: String) = throw new UnparsableContentType(contentType)
  protected def responseNotJson(mimeType: String) = throw new UnexpectedContentType(got = mimeType, expected = jsonContentTypeBase)
  protected def illegalCharsetName(charsetName: String) = throw new IllegalCharsetName(charsetName)
  protected def unsupportedCharset(charsetName: String) = throw new UnsupportedCharset(charsetName)
  protected def noBodyInResponse() = throw new NoBodyInResponse

  private def charsetForJson(responseInfo: ResponseInfo): Charset = responseInfo.headers("content-type") match {
    case Array(ct) =>
      try {
        val mimeType = new MimeType(ct)
        if(mimeType.getBaseType != jsonContentTypeBase) responseNotJson(mimeType.getBaseType)
        Option(mimeType.getParameter("charset")).map(Charset.forName).getOrElse(StandardCharsets.ISO_8859_1)
      } catch {
        case _: MimeTypeParseException =>
          unparsableContentType(ct)
        case e: IllegalCharsetNameException =>
          illegalCharsetName(e.getCharsetName)
        case e: UnsupportedCharsetException =>
          unsupportedCharset(e.getCharsetName)
      }
    case Array() =>
      noContentTypeInResponse()
    case _ =>
      multipleContentTypesInResponse()
  }

  /**
   * Executes the request.
   *
   * @return an `InputStream` with attached HTTP response info.
   */
  def execute(req: SimpleHttpRequest, ping: Option[PingInfo], maximumSizeBetweenAcks: Long = Long.MaxValue): Managed[RawResponse]

  /**
   * Executes the request, checking that the response headers declare the content to be JSON.
   *
   * @return an [[com.socrata.internal.http.Acknowledgeable]] `Reader` with attached HTTP response info.
   */
  def executeExpectingJson(req: SimpleHttpRequest, ping: Option[PingInfo], maximumSizeBetweenAcks: Long = Long.MaxValue): Managed[RawJsonResponse] =
    new SimpleArm[RawJsonResponse] {
      def flatMap[A](f: RawJsonResponse => A): A = {
        for(raw <- execute(req, ping, maximumSizeBetweenAcks)) yield {
          val reader: Reader with Acknowledgeable with ResponseInfoProvider =
            new InputStreamReader(raw, charsetForJson(raw.responseInfo)) with Acknowledgeable with ResponseInfoProvider {
              val responseInfo = raw.responseInfo
              def acknowledge() = raw.acknowledge()
            }
          f(reader)
        }
      }
    }

  /**
   * Executes the request, checking that the response contains JSON.
   *
   * @return an [[com.socrata.internal.http.Acknowledgeable]] `Iterator[JsonEvent]` with attached HTTP response info.
   */
  def executeForJson(req: SimpleHttpRequest, ping: Option[PingInfo], maximumSizeBetweenAcks: Long = Long.MaxValue): Managed[JsonResponse] =
    new SimpleArm[JsonResponse] {
      def flatMap[A](f: JsonResponse => A): A = {
        for(raw <- executeExpectingJson(req, ping, maximumSizeBetweenAcks)) yield {
          val processed: Iterator[JsonEvent] with Acknowledgeable with ResponseInfoProvider =
            new FusedBlockJsonEventIterator(raw) with Acknowledgeable with ResponseInfoProvider {
              val responseInfo = raw.responseInfo
              def acknowledge() = raw.acknowledge()
            }
          f(processed)
        }
      }
    }
}

object HttpClient {
  val jsonContentTypeBase = "application/json"
  val jsonContentType = ContentType.create(jsonContentTypeBase, StandardCharsets.UTF_8)
  val formContentTypeBase = "application/x-www-form-urlencoded"
  val formContentType = ContentType.create(formContentTypeBase)
}

object NoopCloseable extends Closeable {
  def close() {}
}

class HttpClientHttpClient(pingProvider: PingProvider,
                           continueTimeout: Option[Int] = Some(3000),
                           userAgent: String = "HttpClientHttpClient")
  extends HttpClient
{
  import HttpClient._

  private[this] val httpclient = locally {
    val connManager = new PoolingClientConnectionManager
    connManager.setDefaultMaxPerRoute(Int.MaxValue)
    connManager.setMaxTotal(Int.MaxValue)
    new DefaultHttpClient(connManager)
  }
  @volatile private[this] var initialized = false
  private val log = org.slf4j.LoggerFactory.getLogger(classOf[HttpClientHttpClient])
  private val timeoutWorker = new TimeoutWorker

  private class TimeoutWorker extends Thread {
    sealed trait PendingJob
    case class CancelJob(job: Job) extends PendingJob
    case class Job(onTimeout: () => Any, deadline: Long) extends IntrusivePriorityQueueNode with PendingJob with Closeable {
      priority = deadline
      def close() {
        pendingJobs.add(CancelJob(this))
      }
    }
    object PoisonPill extends PendingJob

    private val pendingJobs = new java.util.concurrent.LinkedBlockingQueue[PendingJob]
    private val jobs = new IntrusivePriorityQueue[Job]

    def shutdown() {
      pendingJobs.add(PoisonPill)
      join()
    }

    override def run() {
      while(true) {
        val now = System.currentTimeMillis()
        while(jobs.nonEmpty && jobs.head.deadline <= now) jobs.pop().onTimeout()

        val newJob = if(jobs.nonEmpty) {
          pendingJobs.poll(Math.max(1L, jobs.head.deadline - now), TimeUnit.MILLISECONDS)
        } else {
          pendingJobs.take()
        }

        newJob match {
          case j: Job =>
            jobs.add(j)
          case CancelJob(job) =>
            jobs.remove(job)
          case PoisonPill =>
            if(jobs.nonEmpty) log.warn("Shutting down with " + jobs.size + " timeout jobs remaining")
            return
          case null => // timed out while waiting for the front job
            jobs.pop().onTimeout()
        }
      }
    }

    def addJob(timeout: Int)(onTimeout: => Any): Closeable = {
      val result = Job(() => onTimeout, System.currentTimeMillis() + timeout)
      pendingJobs.add(result)
      result
    }
  }

  private def init() {
    def reallyInit() = synchronized {
      if(!initialized) {
        val params = httpclient.getParams
        HttpProtocolParams.setUserAgent(params, userAgent)
        continueTimeout match {
          case Some(timeout) =>
            HttpProtocolParams.setUseExpectContinue(params, true)
            params.setIntParameter(CoreProtocolPNames.WAIT_FOR_CONTINUE, timeout) // no option for this one?
          case None =>
            HttpProtocolParams.setUseExpectContinue(params, false)
        }
        timeoutWorker.start()
        initialized = true
      }
    }
    if(!initialized) reallyInit()
  }

  def close() {
    try {
      httpclient.getConnectionManager.shutdown()
    } finally {
      timeoutWorker.shutdown()
    }
  }

  private def send[A](req: HttpUriRequest, timeout: Option[Int], pingTarget: Option[PingTarget], maximumSizeBetweenAcks: Long, f: RawResponse => A): A = {
    val LivenessCheck = 0
    val FullTimeout = 1
    @volatile var abortReason: Int = -1 // this can be touched from another thread

    def probablyAborted(e: Exception): Nothing = {
      abortReason match {
        case LivenessCheck => livenessCheckFailed()
        case FullTimeout => fullTimeout()
        case -1 => throw e // wasn't us
        case other => sys.error("Unknown abort reason " + other)
      }
    }

    for {
      _ <- managed {
        pingTarget match {
          case Some(target) => pingProvider.startPinging(target) { abortReason = LivenessCheck; req.abort() }
          case None => NoopCloseable
        }
      }
      _ <- managed {
        timeout match {
          case Some(ms) => timeoutWorker.addJob(ms) { abortReason = FullTimeout; req.abort() }
          case None => NoopCloseable
        }
      }
    } yield {
      val response = try {
        httpclient.execute(req)
      } catch {
        case _: ConnectTimeoutException =>
          connectTimeout()
        case e: ConnectException =>
          connectFailed()
        case e: UndeclaredThrowableException =>
          throw e.getCause
        case e: SocketException if e.getMessage == "Socket closed" =>
          probablyAborted(e)
        case e: IOException if e.getMessage == "Request already aborted" =>
          probablyAborted(e)
        case _: SocketTimeoutException =>
          receiveTimeout()
      }

      val entity = response.getEntity
      if(entity != null) {
        val content = entity.getContent()
        try {
          val catchingInputStream = CatchingInputStream(content) {
            case e: SocketException if e.getMessage == "Socket closed" =>
              probablyAborted(e)
            case e: java.net.SocketTimeoutException =>
              receiveTimeout()
          }
          val processed: InputStream with Acknowledgeable with ResponseInfoProvider =
            new AcknowledgeableInputStream(catchingInputStream, maximumSizeBetweenAcks) with ResponseInfoProvider with ResponseInfo {
              def responseInfo = this
              val resultCode = response.getStatusLine.getStatusCode

              // I am *fairly* sure (from code-diving) that the value field of a header
              // parsed from a response will never be null.
              def headers(name: String) = response.getHeaders(name).map(_.getValue)
              def headerNames = response.getAllHeaders.iterator.map(_.getName).toSet
            }
          f(processed)
        } catch {
          case e: Exception =>
            req.abort()
            throw e
        } finally {
          content.close()
        }
      } else {
        noBodyInResponse()
      }
    }
  }

  def execute(req: SimpleHttpRequest, ping: Option[PingInfo], maximumSizeBetweenAcks: Long): Managed[RawResponse] =
    req match {
      case bodyless: BodylessHttpRequest => processBodyless(bodyless, ping, maximumSizeBetweenAcks)
      case form: FormHttpRequest => processForm(form, ping, maximumSizeBetweenAcks)
      case file: FileHttpRequest => processFile(file, ping, maximumSizeBetweenAcks)
      case json: JsonHttpRequest => processJson(json, ping, maximumSizeBetweenAcks)
    }

  def pingTarget(req: SimpleHttpRequest, ping: Option[PingInfo]): Option[PingTarget] = ping match {
    case Some(pi) => Some(new PingTarget(InetAddress.getByName(req.builder.host), pi.port, pi.response))
    case None => None
  }

  def setupOp(req: SimpleHttpRequest, op: HttpRequestBase) {
    for((k, v) <- req.builder.headers) op.addHeader(k, v)
    val params = op.getParams
    req.builder.connectTimeoutMS.foreach { ms =>
      HttpConnectionParams.setConnectionTimeout(params, ms)
    }
    req.builder.receiveTimeoutMS.foreach { ms =>
      HttpConnectionParams.setSoTimeout(params, ms)
    }
  }

  def bodylessOp(req: SimpleHttpRequest): HttpRequestBase = req.builder.method match {
    case Some(m) =>
      val op = new HttpRequestBase {
        setURI(new URI(req.builder.url))
        def getMethod = m
      }
      setupOp(req, op)
      op
    case None =>
      throw new IllegalArgumentException("No method in request")
  }

  def bodyEnclosingOp(req: SimpleHttpRequest): HttpEntityEnclosingRequestBase = req.builder.method match {
    case Some(m) =>
      val op = new HttpEntityEnclosingRequestBase {
        setURI(new URI(req.builder.url))
        def getMethod = m
      }
      setupOp(req, op)
      op
    case None =>
      throw new IllegalArgumentException("No method in request")
  }

  def processBodyless(req: BodylessHttpRequest, ping: Option[PingInfo], maximumSizeBetweenAcks: Long) = new SimpleArm[RawResponse] {
    def flatMap[A](f: RawResponse => A): A = {
      init()
      val op = bodylessOp(req)
      send(op, req.builder.timeoutMS, pingTarget(req, ping), maximumSizeBetweenAcks, f)
    }
  }

  def processForm(req: FormHttpRequest, ping: Option[PingInfo], maximumSizeBetweenAcks: Long): Managed[RawResponse] = new SimpleArm[RawResponse] {
    def flatMap[A](f: RawResponse => A): A = {
      init()
      val sendEntity = new InputStreamEntity(new ReaderInputStream(new FormReader(req.contents), StandardCharsets.UTF_8), -1, formContentType)
      sendEntity.setChunked(true)
      val op = bodyEnclosingOp(req)
      op.setEntity(sendEntity)
      send(op, req.builder.timeoutMS, pingTarget(req, ping), maximumSizeBetweenAcks, f)
    }
  }

  def processFile(req: FileHttpRequest, ping: Option[PingInfo], maximumSizeBetweenAcks: Long): Managed[RawResponse] = new SimpleArm[RawResponse] {
    def flatMap[A](f: RawResponse => A): A = {
      init()
      val sendEntity = new MultipartEntity
      sendEntity.addPart(req.field, new InputStreamBody(req.contents, req.contentType, req.file))
      val op = bodyEnclosingOp(req)
      op.setEntity(sendEntity)
      send(op, req.builder.timeoutMS, pingTarget(req, ping), maximumSizeBetweenAcks, f)
    }
  }

  def processJson(req: JsonHttpRequest, ping: Option[PingInfo], maximumSizeBetweenAcks: Long): Managed[RawResponse] = new SimpleArm[RawResponse] {
    def flatMap[A](f: RawResponse => A): A = {
      init()
      val sendEntity = new InputStreamEntity(new ReaderInputStream(new JsonEventIteratorReader(req.contents), StandardCharsets.UTF_8), -1, jsonContentType)
      sendEntity.setChunked(true)
      val op = bodyEnclosingOp(req)
      op.setEntity(sendEntity)
      send(op, req.builder.timeoutMS, pingTarget(req, ping), maximumSizeBetweenAcks, f)
    }
  }
}

object TimeoutTest extends App {
  using(new HttpClientHttpClient(NoopPingProvider)) { cli =>
    val req = SimpleHttpRequestBuilder("localhost").port(6060).timeoutMS(Some(5000)).get
    for(x <- cli.execute(req, None)) {
      println(x.responseInfo.resultCode)
    }
  }
}
