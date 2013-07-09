package com.socrata.simplehttp

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
import org.apache.http.{HttpEntity, HttpResponse}
import javax.activation.{MimeTypeParseException, MimeType}
import java.lang.reflect.UndeclaredThrowableException
import com.rojoma.simplearm
import org.apache.http.entity.mime.{FormBodyPart, MultipartEntity}
import org.apache.http.entity.mime.content.InputStreamBody
import org.apache.http.impl.conn.PoolingClientConnectionManager
import org.apache.http.params.{CoreProtocolPNames, HttpProtocolParams, HttpConnectionParams}
import scala.Some

trait ResponseInfo {
  def resultCode: Int
  def headers(name: String): Array[String] // this will return an empty array if the header does not exist
}

trait ResponseInfoProvider {
  def responseInfo: ResponseInfo
}

trait HttpClient extends Closeable {
  import HttpClient._

  type Response = Iterator[JsonEvent] with ResponseInfoProvider
  type RawResponse = InputStream with ResponseInfoProvider

  protected def noContentTypeInResponse() = ???
  protected def unparsableContentType(contentType: String) = ???
  protected def responseNotJson(mimeType: String) = ???
  protected def illegalCharsetName(charsetName: String) = ???
  protected def unsupportedCharset(charsetName: String) = ???
  protected def noBodyInResponse() = ???

  protected def charsetFor(responseInfo: ResponseInfo): Charset = responseInfo.headers("content-type").headOption match {
    case Some(ct) =>
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
    case None =>
      noContentTypeInResponse()
  }

  private def jsonify(response: Managed[RawResponse]): Managed[Response] = new SimpleArm[Response] {
    def flatMap[A](f: Response => A): A = {
      for(raw <- response) yield {
        val reader = new InputStreamReader(raw, charsetFor(raw.responseInfo))
        val processed: Iterator[JsonEvent] with ResponseInfoProvider =
          new FusedBlockJsonEventIterator(reader) with ResponseInfoProvider {
            def responseInfo = raw.responseInfo
          }
        f(processed)
      }
    }
  }

  def getRaw(url: SimpleURL): Managed[RawResponse]
  def get(url: SimpleURL) = jsonify(getRaw(url))

  def deleteRaw(url: SimpleURL): Managed[RawResponse]
  def delete(url: SimpleURL) = jsonify(deleteRaw(url))

  def postJsonRaw[T : JsonCodec](url: SimpleURL, body: T): Managed[RawResponse]
  def postJsonRaw(url: SimpleURL, body: Iterator[JsonEvent]): Managed[RawResponse]
  def postFormRaw(url: SimpleURL, formContents: Iterable[(String, String)]): Managed[RawResponse]
  def postFileRaw(url: SimpleURL, input: Managed[InputStream], file: String = "file", field: String = "file", contentType: String = octetStreamContentTypeBase): Managed[RawResponse]
  def postJson[T : JsonCodec](url: SimpleURL, body: T) = jsonify(postJsonRaw(url, body))
  def postJson(url: SimpleURL, body: Iterator[JsonEvent]) = jsonify(postJsonRaw(url, body))
  def postForm(url: SimpleURL, formContents: Iterable[(String, String)]) = jsonify(postFormRaw(url, formContents))
  def postFile(url: SimpleURL, input: Managed[InputStream], file: String = "file", field: String = "file", contentType: String = octetStreamContentTypeBase) =
    jsonify(postFileRaw(url, input, file, field, contentType))

  def putJsonRaw[T : JsonCodec](url: SimpleURL, body: T): Managed[RawResponse]
  def putJsonRaw(url: SimpleURL, body: Iterator[JsonEvent]): Managed[RawResponse]
  def putFileRaw(url: SimpleURL, input: Managed[InputStream], file: String = "file", field: String = "file", contentType: String = octetStreamContentTypeBase): Managed[RawResponse]
  def putJson[T : JsonCodec](url: SimpleURL, body: T) = jsonify(putJsonRaw(url, body))
  def putJson(url: SimpleURL, body: Iterator[JsonEvent]) = jsonify(putJsonRaw(url, body))
  def putFile(url: SimpleURL, input: Managed[InputStream], file: String = "file", field: String = "file", contentType: String = octetStreamContentTypeBase) =
    jsonify(putFileRaw(url, input, file, field, contentType))
}

object HttpClient {
  val jsonContentTypeBase = "application/json"
  val jsonContentType = ContentType.create(jsonContentTypeBase, StandardCharsets.UTF_8)
  val formContentTypeBase = "application/x-www-form-urlencoded"
  val formContentType = ContentType.create(formContentTypeBase)
  val octetStreamContentTypeBase = "application/octet-stream"
  val octetStreamContentType = ContentType.create(octetStreamContentTypeBase)

}

class HttpClientHttpClient(val connectionTimeout: Int,
                           val dataTimeout: Int,
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

  private def init() {
    if(!initialized) {
      synchronized {
        if(!initialized) {
          val params = httpclient.getParams
          HttpConnectionParams.setConnectionTimeout(params, connectionTimeout)
          HttpConnectionParams.setSoTimeout(params, dataTimeout)
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
    }
  }

  def close() {
    httpclient.getConnectionManager.shutdown()
  }

  private def send[A](req: HttpUriRequest, f: RawResponse => A): A = {
    val response = try {
      httpclient.execute(req)
    } catch {
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

  def getRaw(url: SimpleURL): Managed[RawResponse] = new SimpleArm[RawResponse] {
    def flatMap[A](f: RawResponse => A): A = {
      init()
      send(new HttpGet(url.toString), f)
    }
  }

  def deleteRaw(url: SimpleURL): Managed[RawResponse] = new SimpleArm[RawResponse] {
    def flatMap[A](f: RawResponse => A): A = {
      init()
      send(new HttpDelete(url.toString), f)
    }
  }

  def postJsonRaw[T: JsonCodec](url: SimpleURL, body: T): Managed[RawResponse] =
    postJsonRaw(url, JValueEventIterator(JsonCodec[T].encode(body)))

  def postJsonRaw(url: SimpleURL, body: Iterator[JsonEvent]): Managed[RawResponse] =
    streamBody(url, body, new HttpPost(_))

  def postFormRaw(url: SimpleURL, formContents: Iterable[(String, String)]): Managed[RawResponse] = new SimpleArm[RawResponse] {
    def flatMap[A](f: RawResponse => A): A = {
      init()
      val sendEntity = new InputStreamEntity(new ReaderInputStream(new FormReader(formContents), StandardCharsets.UTF_8), -1, formContentType)
      sendEntity.setChunked(true)
      val post = new HttpPost(url.toString)
      post.setEntity(sendEntity)
      send(post, f)
    }
  }

  def postFileRaw(url: SimpleURL, input: Managed[InputStream], file: String, field: String, contentType: String): Managed[RawResponse] =
    streamFile(url, input, file, field, contentType, new HttpPost(_))

  def putJsonRaw[T: JsonCodec](url: SimpleURL, body: T): Managed[RawResponse] =
    putJsonRaw(url, JValueEventIterator(JsonCodec[T].encode(body)))

  def putJsonRaw(url: SimpleURL, body: Iterator[JsonEvent]): Managed[RawResponse] =
    streamBody(url, body, new HttpPut(_))

  def putFileRaw(url: SimpleURL, input: Managed[InputStream], file: String, field: String, contentType: String): Managed[RawResponse] =
    streamFile(url, input, file, field, contentType, new HttpPut(_))

  private def streamBody(url: SimpleURL, body: Iterator[JsonEvent], method: String => HttpEntityEnclosingRequestBase): Managed[RawResponse] = new SimpleArm[RawResponse] {
    def flatMap[A](f: RawResponse => A): A = {
      init()
      val sendEntity = new InputStreamEntity(new ReaderInputStream(new JsonEventIteratorReader(body), StandardCharsets.UTF_8), -1, jsonContentType)
      sendEntity.setChunked(true)
      val op = method(url.toString)
      op.setEntity(sendEntity)
      send(op, f)
    }
  }

  private def streamFile(url: SimpleURL, input: Managed[InputStream], file: String, field: String, contentType: String, method: String => HttpEntityEnclosingRequestBase): Managed[RawResponse] =
    new SimpleArm[RawResponse] {
      def flatMap[A](f: RawResponse => A): A = {
        init()
        for(inputStream <- input) yield {
          val sendEntity = new MultipartEntity
          sendEntity.addPart(field, new InputStreamBody(inputStream, contentType, file))
          val op = method(url.toString)
          op.setEntity(sendEntity)
          send(op, f)
        }
      }
    }
}

object Blah extends App {
  for {
    cli <- managed[HttpClient](new HttpClientHttpClient(100000, 100000, continueTimeout = None))
    compressed <- managed(new FileInputStream("/home/robertm/car_linej_lds_5_2011.small.mjson.gz"))
    uncompressed <- managed(new GZIPInputStream(compressed))
    reader <- managed(new InputStreamReader(uncompressed, StandardCharsets.UTF_8))
    resp <- cli.postJson(SimpleURL.http("localhost", port = 10000), new FusedBlockJsonEventIterator(reader))
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
