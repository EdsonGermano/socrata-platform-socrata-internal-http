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
import java.net.ConnectException
import java.util.concurrent.{Executors, ExecutorService}
import com.socrata.internal.http.pingpong.{InetPingProvider, PingTarget, PingProvider}

trait ResponseInfo {
  def resultCode: Int
  def headers(name: String): Array[String] // this will return an empty array if the header does not exist
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

  def getRaw(url: SimpleURL, ping: Option[PingTarget]): Managed[RawResponse]
  def get(url: SimpleURL, ping: Option[PingTarget]) = jsonify(getRaw(url, ping))

  def deleteRaw(url: SimpleURL, ping: Option[PingTarget]): Managed[RawResponse]
  def delete(url: SimpleURL, ping: Option[PingTarget]) = jsonify(deleteRaw(url, ping))

  def postJsonRaw[T : JsonCodec](url: SimpleURL, ping: Option[PingTarget], body: T): Managed[RawResponse]
  def postJsonRaw(url: SimpleURL, ping: Option[PingTarget], body: Iterator[JsonEvent]): Managed[RawResponse]
  def postFormRaw(url: SimpleURL, ping: Option[PingTarget], formContents: Iterable[(String, String)]): Managed[RawResponse]
  def postFileRaw(url: SimpleURL, ping: Option[PingTarget], input: Managed[InputStream], file: String = "file", field: String = "file", contentType: String = octetStreamContentTypeBase): Managed[RawResponse]
  def postJson[T : JsonCodec](url: SimpleURL, ping: Option[PingTarget], body: T) = jsonify(postJsonRaw(url, ping, body))
  def postJson(url: SimpleURL, ping: Option[PingTarget], body: Iterator[JsonEvent]) = jsonify(postJsonRaw(url, ping, body))
  def postForm(url: SimpleURL, ping: Option[PingTarget], formContents: Iterable[(String, String)]) = jsonify(postFormRaw(url, ping, formContents))
  def postFile(url: SimpleURL, ping: Option[PingTarget], input: Managed[InputStream], file: String = "file", field: String = "file", contentType: String = octetStreamContentTypeBase) =
    jsonify(postFileRaw(url, ping, input, file, field, contentType))

  def putJsonRaw[T : JsonCodec](url: SimpleURL, ping: Option[PingTarget], body: T): Managed[RawResponse]
  def putJsonRaw(url: SimpleURL, ping: Option[PingTarget], body: Iterator[JsonEvent]): Managed[RawResponse]
  def putFileRaw(url: SimpleURL, ping: Option[PingTarget], input: Managed[InputStream], file: String = "file", field: String = "file", contentType: String = octetStreamContentTypeBase): Managed[RawResponse]
  def putJson[T : JsonCodec](url: SimpleURL, ping: Option[PingTarget], body: T) = jsonify(putJsonRaw(url, ping, body))
  def putJson(url: SimpleURL, ping: Option[PingTarget], body: Iterator[JsonEvent]) = jsonify(putJsonRaw(url, ping, body))
  def putFile(url: SimpleURL, ping: Option[PingTarget], input: Managed[InputStream], file: String = "file", field: String = "file", contentType: String = octetStreamContentTypeBase) =
    jsonify(putFileRaw(url, ping, input, file, field, contentType))
}

object HttpClient {
  val jsonContentTypeBase = "application/json"
  val jsonContentType = ContentType.create(jsonContentTypeBase, StandardCharsets.UTF_8)
  val formContentTypeBase = "application/x-www-form-urlencoded"
  val formContentType = ContentType.create(formContentTypeBase)
  val octetStreamContentTypeBase = "application/octet-stream"
  val octetStreamContentType = ContentType.create(octetStreamContentTypeBase)
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

  def getRaw(url: SimpleURL, ping: Option[PingTarget]): Managed[RawResponse] = new SimpleArm[RawResponse] {
    def flatMap[A](f: RawResponse => A): A = {
      init()
      send(new HttpGet(url.toString), ping, f)
    }
  }

  def deleteRaw(url: SimpleURL, ping: Option[PingTarget]): Managed[RawResponse] = new SimpleArm[RawResponse] {
    def flatMap[A](f: RawResponse => A): A = {
      init()
      send(new HttpDelete(url.toString), ping, f)
    }
  }

  def postJsonRaw[T: JsonCodec](url: SimpleURL, ping: Option[PingTarget], body: T): Managed[RawResponse] =
    postJsonRaw(url, ping, JValueEventIterator(JsonCodec[T].encode(body)))

  def postJsonRaw(url: SimpleURL, ping: Option[PingTarget], body: Iterator[JsonEvent]): Managed[RawResponse] =
    streamBody(url, ping, body, new HttpPost(_))

  def postFormRaw(url: SimpleURL, ping: Option[PingTarget], formContents: Iterable[(String, String)]): Managed[RawResponse] = new SimpleArm[RawResponse] {
    def flatMap[A](f: RawResponse => A): A = {
      init()
      val sendEntity = new InputStreamEntity(new ReaderInputStream(new FormReader(formContents), StandardCharsets.UTF_8), -1, formContentType)
      sendEntity.setChunked(true)
      val post = new HttpPost(url.toString)
      post.setEntity(sendEntity)
      send(post, ping, f)
    }
  }

  def postFileRaw(url: SimpleURL, ping: Option[PingTarget], input: Managed[InputStream], file: String, field: String, contentType: String): Managed[RawResponse] =
    streamFile(url, ping, input, file, field, contentType, new HttpPost(_))

  def putJsonRaw[T: JsonCodec](url: SimpleURL, ping: Option[PingTarget], body: T): Managed[RawResponse] =
    putJsonRaw(url, ping, JValueEventIterator(JsonCodec[T].encode(body)))

  def putJsonRaw(url: SimpleURL, ping: Option[PingTarget], body: Iterator[JsonEvent]): Managed[RawResponse] =
    streamBody(url, ping, body, new HttpPut(_))

  def putFileRaw(url: SimpleURL, ping: Option[PingTarget], input: Managed[InputStream], file: String, field: String, contentType: String): Managed[RawResponse] =
    streamFile(url, ping, input, file, field, contentType, new HttpPut(_))

  private def streamBody(url: SimpleURL, ping: Option[PingTarget], body: Iterator[JsonEvent], method: String => HttpEntityEnclosingRequestBase): Managed[RawResponse] = new SimpleArm[RawResponse] {
    def flatMap[A](f: RawResponse => A): A = {
      init()
      val sendEntity = new InputStreamEntity(new ReaderInputStream(new JsonEventIteratorReader(body), StandardCharsets.UTF_8), -1, jsonContentType)
      sendEntity.setChunked(true)
      val op = method(url.toString)
      op.setEntity(sendEntity)
      send(op, ping, f)
    }
  }

  private def streamFile(url: SimpleURL, ping: Option[PingTarget], input: Managed[InputStream], file: String, field: String, contentType: String, method: String => HttpEntityEnclosingRequestBase): Managed[RawResponse] =
    new SimpleArm[RawResponse] {
      def flatMap[A](f: RawResponse => A): A = {
        init()
        for(inputStream <- input) yield {
          val sendEntity = new MultipartEntity
          sendEntity.addPart(field, new InputStreamBody(inputStream, contentType, file))
          val op = method(url.toString)
          op.setEntity(sendEntity)
          send(op, ping, f)
        }
      }
    }
}

object Blah extends App {
  import java.net._
  import scala.concurrent.duration._
  implicit object ExecutorServiceResource extends com.rojoma.simplearm.Resource[ExecutorService] {
    def close(a: ExecutorService) { a.shutdown() }
  }

  val ping = new PingTarget(InetAddress.getLoopbackAddress, 12345, "hello".getBytes)
  for {
    executor <- managed(Executors.newCachedThreadPool())
    pingProvider <- managed(new InetPingProvider(5.seconds, 1.second, 5, executor))
    _ <- unmanaged(pingProvider.start())
    cli <- managed[HttpClient](new HttpClientHttpClient(pingProvider, continueTimeout = None))
    compressed <- managed(new FileInputStream("/home/robertm/car_linej_lds_5_2011.small.mjson.gz"))
    uncompressed <- managed(new GZIPInputStream(compressed))
    reader <- managed(new InputStreamReader(uncompressed, StandardCharsets.UTF_8))
    resp <- cli.postJson(SimpleURL.http("localhost", port = 10000), Some(ping), new FusedBlockJsonEventIterator(reader))
    /*
    compressed <- managed(new FileInputStream("/home/robertm/tiny.gz"))
    resp <- cli.postFile(SimpleURL.http("localhost", port = 10000), managed(new GZIPInputStream(compressed)))
    */
  } {
    println(resp.responseInfo.resultCode)
    EventTokenIterator(resp).map(_.asFragment).foreach(print)
    println()
  }
}
