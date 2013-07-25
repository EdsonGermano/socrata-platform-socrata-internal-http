package com.socrata.internal.http

import java.io._
import java.nio.charset.StandardCharsets
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.client.methods._
import org.apache.http.entity._
import com.rojoma.simplearm._
import com.rojoma.simplearm.util._
import java.lang.reflect.UndeclaredThrowableException
import org.apache.http.entity.mime.MultipartEntity
import org.apache.http.entity.mime.content.InputStreamBody
import org.apache.http.impl.conn.PoolingClientConnectionManager
import org.apache.http.params.{CoreProtocolPNames, HttpProtocolParams, HttpConnectionParams}
import org.apache.http.conn.ConnectTimeoutException
import java.net._
import com.socrata.internal.http.pingpong._
import java.util.concurrent.Executors
import com.socrata.internal.http.util._
import scala.Some

trait ResponseInfo {
  def resultCode: Int
  def headers(name: String): Array[String] // this will return an empty array if the header does not exist
  def headerNames: Set[String]
}

trait ResponseInfoProvider {
  def responseInfo: ResponseInfo
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
  private val timeoutManager = new TimeoutManager(Executors.newSingleThreadExecutor)

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
        timeoutManager.start()
        initialized = true
      }
    }
    if(!initialized) reallyInit()
  }

  def close() {
    try {
      httpclient.getConnectionManager.shutdown()
    } finally {
      timeoutManager.close()
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
          case Some(ms) => timeoutManager.addJob(ms) { abortReason = FullTimeout; req.abort() }
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
    val req = RequestBuilder("localhost").port(6060).timeoutMS(Some(5000)).get
    for(x <- cli.execute(req, None)) {
      println(x.responseInfo.resultCode)
    }
  }
}
