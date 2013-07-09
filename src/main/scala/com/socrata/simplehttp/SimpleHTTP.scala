package com.socrata.simplehttp

import java.io.{InputStream, FileInputStream, Closeable, InputStreamReader}
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

trait ResponseInfo {
  def resultCode: Int
}

trait ResponseInfoProvider {
  def responseInfo: ResponseInfo
}

trait HttpClient extends Closeable {
  import HttpClient._

  type Response = Iterator[JsonEvent] with ResponseInfoProvider

  def get(url: SimpleURL): Managed[Response]

  def delete(url: SimpleURL): Managed[Response]

  def post[T : JsonCodec](url: SimpleURL, body: T): Managed[Response]
  def post(url: SimpleURL, body: Iterator[JsonEvent]): Managed[Response]
  def post(url: SimpleURL, formContents: Iterable[(String, String)]): Managed[Response]
  def postFile(url: SimpleURL, input: Managed[InputStream], file: String = "file", field: String = "file", contentType: String = octetStreamContentTypeBase): Managed[Response]

  def put[T : JsonCodec](url: SimpleURL, body: T): Managed[Response]
  def put(url: SimpleURL, body: Iterator[JsonEvent]): Managed[Response]
  def putFile(url: SimpleURL, input: Managed[InputStream], file: String = "file", field: String = "file", contentType: String = octetStreamContentTypeBase): Managed[Response]
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

  def noContentTypeInResponse() = ???
  def unparsableContentType(contentType: String) = ???
  def responseNotJson(mimeType: String) = ???
  def illegalCharsetName(charsetName: String) = ???
  def unsupportedCharset(charsetName: String) = ???
  def noBodyInResponse() = ???

  private def responsify(response: HttpResponse): Iterator[JsonEvent] with ResponseInfoProvider = {
    val entity = response.getEntity
    val contentType = entity.getContentType
    if(contentType == null) noContentTypeInResponse()

    val charset = try {
      val mimeType = new MimeType(contentType.getValue)
      if(mimeType.getBaseType != jsonContentTypeBase) responseNotJson(mimeType.getBaseType)
      Option(mimeType.getParameter("charset")).map(Charset.forName).getOrElse(StandardCharsets.ISO_8859_1)
    } catch {
      case _: MimeTypeParseException =>
        unparsableContentType(contentType.getValue)
      case e: IllegalCharsetNameException =>
        illegalCharsetName(e.getCharsetName)
      case e: UnsupportedCharsetException =>
        unsupportedCharset(e.getCharsetName)
    }

    val reader = new InputStreamReader(entity.getContent, charset)
    new FusedBlockJsonEventIterator(reader) with ResponseInfoProvider with ResponseInfo {
      def responseInfo = this
      val resultCode = response.getStatusLine.getStatusCode
    }
  }

  private def send[A](req: HttpUriRequest, f: Response => A) = {
    val response = try {
      httpclient.execute(req)
    } catch {
      case e: UndeclaredThrowableException =>
        throw e.getCause
    }
    if(response.getEntity != null) {
      val content = response.getEntity.getContent
      try {
        f(responsify(response))
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

  def get(url: SimpleURL): Managed[Response] = new SimpleArm[Response] {
    def flatMap[A](f: Response => A): A = {
      init()
      send(new HttpGet(url.toString), f)
    }
  }

  def delete(url: SimpleURL): Managed[Response] = new SimpleArm[Response] {
    def flatMap[A](f: Response => A): A = {
      init()
      send(new HttpDelete(url.toString), f)
    }
  }

  def post[T: JsonCodec](url: SimpleURL, body: T): Managed[Response] =
    post(url, JValueEventIterator(JsonCodec[T].encode(body)))

  def post(url: SimpleURL, body: Iterator[JsonEvent]): Managed[Response] =
    streamBody(url, body, new HttpPost(_))

  def post(url: SimpleURL, formContents: Iterable[(String, String)]): Managed[Response] = new SimpleArm[Response] {
    def flatMap[A](f: Response => A): A = {
      init()
      val sendEntity = new InputStreamEntity(new ReaderInputStream(new FormReader(formContents), StandardCharsets.UTF_8), -1, formContentType)
      sendEntity.setChunked(true)
      val post = new HttpPost(url.toString)
      post.setEntity(sendEntity)
      send(post, f)
    }
  }

  def postFile(url: SimpleURL, input: Managed[InputStream], file: String, field: String, contentType: String): Managed[Response] =
    streamFile(url, input, file, field, contentType, new HttpPost(_))

  def put[T: JsonCodec](url: SimpleURL, body: T): Managed[Response] =
    put(url, JValueEventIterator(JsonCodec[T].encode(body)))

  def put(url: SimpleURL, body: Iterator[JsonEvent]): Managed[Response] =
    streamBody(url, body, new HttpPut(_))

  def putFile(url: SimpleURL, input: Managed[InputStream], file: String, field: String, contentType: String): Managed[Response] =
    streamFile(url, input, file, field, contentType, new HttpPut(_))

  private def streamBody(url: SimpleURL, body: Iterator[JsonEvent], method: String => HttpEntityEnclosingRequestBase): Managed[Response] = new SimpleArm[Response] {
    def flatMap[A](f: Response => A): A = {
      init()
      val sendEntity = new InputStreamEntity(new ReaderInputStream(new JsonEventIteratorReader(body), StandardCharsets.UTF_8), -1, jsonContentType)
      sendEntity.setChunked(true)
      val op = method(url.toString)
      op.setEntity(sendEntity)
      send(op, f)
    }
  }

  private def streamFile(url: SimpleURL, input: Managed[InputStream], file: String, field: String, contentType: String, method: String => HttpEntityEnclosingRequestBase): Managed[Response] =
    new SimpleArm[Response] {
      def flatMap[A](f: Response => A): A = {
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
    cli <- managed[HttpClient](new HttpClientHttpClient(100000, 100000))
    /*
    compressed <- managed(new FileInputStream("/home/robertm/car_linej_lds_5_2011.small.mjson.gz"))
    uncompressed <- managed(new GZIPInputStream(compressed))
    reader <- managed(new InputStreamReader(uncompressed, StandardCharsets.UTF_8))
    resp <- cli.post(SimpleURL.http("localhost", port = 10000), new FusedBlockJsonEventIterator(reader))
    */
    compressed <- managed(new FileInputStream("/home/robertm/tiny.gz"))
    resp <- cli.postFile(SimpleURL.http("localhost", port = 10000), managed(new GZIPInputStream(compressed)))
  } {
    println(resp.responseInfo.resultCode)
    println(JsonReader.fromEvents(resp))
    throw new Exception("hello world")
  }
}
