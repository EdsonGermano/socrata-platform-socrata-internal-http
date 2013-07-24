package com.socrata.internal.http

import java.io._
import java.nio.charset.{UnsupportedCharsetException, IllegalCharsetNameException, Charset, StandardCharsets}
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.client.methods._
import org.apache.http.entity._
import com.rojoma.simplearm._
import com.rojoma.simplearm.util._
import com.rojoma.json.io._
import com.rojoma.json.codec.JsonCodec
import java.util.zip.GZIPInputStream
import javax.activation.{MimeTypeParseException, MimeType}
import java.lang.reflect.UndeclaredThrowableException
import org.apache.http.entity.mime.MultipartEntity
import org.apache.http.entity.mime.content.InputStreamBody
import org.apache.http.impl.conn.PoolingClientConnectionManager
import org.apache.http.params.{CoreProtocolPNames, HttpProtocolParams, HttpConnectionParams}
import org.apache.http.conn.ConnectTimeoutException
import java.net.{URI, InetAddress, ConnectException}
import java.util.concurrent.{Executors, ExecutorService}
import com.socrata.internal.http.pingpong.{PingInfo, InetPingProvider, PingTarget, PingProvider}

trait ResponseInfo {
  def resultCode: Int
  def headers(name: String): Array[String] // this will return an empty array if the header does not exist
  def headerNames: Set[String]
}

trait ResponseInfoProvider {
  def responseInfo: ResponseInfo
}

class HttpClientException extends Exception
class ConnectTimeout extends HttpClientException
class ConnectFailed extends HttpClientException // failed for not-timeout reasons
class ReceiveTimeout extends HttpClientException
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

  type Response = Iterator[JsonEvent] with ResponseInfoProvider
  type RawResponse = InputStream with ResponseInfoProvider

  protected def connectTimeout() = throw new ConnectTimeout
  protected def connectFailed() = throw new ConnectFailed
  protected def noContentTypeInResponse() = throw new NoContentTypeInResponse
  protected def multipleContentTypesInResponse() = throw new MultipleContentTypesInResponse
  protected def unparsableContentType(contentType: String) = throw new UnparsableContentType(contentType)
  protected def responseNotJson(mimeType: String) = throw new UnexpectedContentType(got = mimeType, expected = jsonContentTypeBase)
  protected def illegalCharsetName(charsetName: String) = throw new IllegalCharsetName(charsetName)
  protected def unsupportedCharset(charsetName: String) = throw new UnsupportedCharset(charsetName)
  protected def noBodyInResponse() = throw new NoBodyInResponse

  protected def charsetFor(responseInfo: ResponseInfo): Charset = responseInfo.headers("content-type") match {
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

  private def jsonify(response: Managed[RawResponse]): Managed[Response] = new SimpleArm[Response] {
    def flatMap[A](f: Response => A): A = {
      for(raw <- response) yield {
        val reader = new InputStreamReader(raw, charsetFor(raw.responseInfo))
        val processed: Iterator[JsonEvent] with ResponseInfoProvider =
          new FusedBlockJsonEventIterator(reader) with ResponseInfoProvider {
            val responseInfo = raw.responseInfo
          }
        f(processed)
      }
    }
  }

  def processRaw(req: SimpleHttpRequest, ping: Option[PingInfo]): Managed[RawResponse]
  def process(req: SimpleHttpRequest, ping: Option[PingInfo]) = jsonify(processRaw(req, ping))
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
        initialized = true
      }
    }
    if(!initialized) reallyInit()
  }

  def close() {
    httpclient.getConnectionManager.shutdown()
  }

  private def send[A](req: HttpUriRequest, pingTarget: Option[PingTarget], f: RawResponse => A): A = {
    for {
      _ <- managed {
        pingTarget match {
          case Some(target) => pingProvider.startPinging(target) { req.abort() }
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
      }

      val entity = response.getEntity
      if(entity != null) {
        val content = entity.getContent
        try {
          val processed: InputStream with ResponseInfoProvider =
            new FilterInputStream(content) with ResponseInfoProvider with ResponseInfo {
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

  def processRaw(req: SimpleHttpRequest, ping: Option[PingInfo]): Managed[RawResponse] =
    req match {
      case bodyless: BodylessHttpRequest => processBodyless(bodyless, ping)
      case form: FormHttpRequest => processForm(form, ping)
      case file: FileHttpRequest => processFile(file, ping)
      case json: JsonHttpRequest => processJson(json, ping)
    }

  def pingTarget(req: SimpleHttpRequest, ping: Option[PingInfo]): Option[PingTarget] = ping match {
    case Some(pi) => Some(new PingTarget(InetAddress.getByName(req.builder.host), pi.port, pi.response))
    case None => None
  }

  def setupOp(req: SimpleHttpRequest, op: HttpRequestBase) {
    for((k, v) <- req.builder.headers) op.setHeader(k, v)
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

  def processBodyless(req: BodylessHttpRequest, ping: Option[PingInfo]) = new SimpleArm[RawResponse] {
    def flatMap[A](f: RawResponse => A): A = {
      init()
      val op = bodylessOp(req)
      send(op, pingTarget(req, ping), f)
    }
  }

  def processForm(req: FormHttpRequest, ping: Option[PingInfo]): Managed[RawResponse] = new SimpleArm[RawResponse] {
    def flatMap[A](f: RawResponse => A): A = {
      init()
      val sendEntity = new InputStreamEntity(new ReaderInputStream(new FormReader(req.contents), StandardCharsets.UTF_8), -1, formContentType)
      sendEntity.setChunked(true)
      val op = bodyEnclosingOp(req)
      op.setEntity(sendEntity)
      send(op, pingTarget(req, ping), f)
    }
  }

  def processFile(req: FileHttpRequest, ping: Option[PingInfo]): Managed[RawResponse] = new SimpleArm[RawResponse] {
    def flatMap[A](f: RawResponse => A): A = {
      init()
      val sendEntity = new MultipartEntity
      sendEntity.addPart(req.field, new InputStreamBody(req.contents, req.contentType, req.file))
      val op = bodyEnclosingOp(req)
      op.setEntity(sendEntity)
      send(op, pingTarget(req, ping), f)
    }
  }

  def processJson(req: JsonHttpRequest, ping: Option[PingInfo]): Managed[RawResponse] = new SimpleArm[RawResponse] {
    def flatMap[A](f: RawResponse => A): A = {
      init()
      val sendEntity = new InputStreamEntity(new ReaderInputStream(new JsonEventIteratorReader(req.contents), StandardCharsets.UTF_8), -1, jsonContentType)
      sendEntity.setChunked(true)
      val op = bodyEnclosingOp(req)
      op.setEntity(sendEntity)
      send(op, pingTarget(req, ping), f)
    }
  }
}


