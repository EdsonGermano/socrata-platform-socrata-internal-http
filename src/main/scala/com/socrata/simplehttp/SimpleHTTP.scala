package com.socrata.simplehttp

import java.io.{FileInputStream, Closeable, InputStreamReader}
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

trait ResponseInfo {
  def resultCode: Int
}

trait ResponseInfoProvider {
  def responseInfo: ResponseInfo
}

trait HttpClient extends Closeable {
  type Response = Iterator[JsonEvent] with ResponseInfoProvider

  def get(url: SimpleURL): Managed[Response]
  def delete(url: SimpleURL): Managed[Response]
  def post[T : JsonCodec](url: SimpleURL, body: T): Managed[Response]
  def post(url: SimpleURL, body: Iterator[JsonEvent]): Managed[Response]
  def put[T : JsonCodec](url: SimpleURL, body: T): Managed[Response]
  def put(url: SimpleURL, body: Iterator[JsonEvent]): Managed[Response]
}

class HttpClientHttpClient(val connectionTimeout: Int, val dataTimeout: Int, continueTimeout: Option[Int] = Some(3000)) extends HttpClient {
  private[this] val httpclient = new DefaultHttpClient
  @volatile private[this] var initialized = false

  private[this] val jsonContentTypeBase = "application/json"
  private[this] val jsonContentType = ContentType.create(jsonContentTypeBase, StandardCharsets.UTF_8)

  private def init() {
    if(!initialized) {
      synchronized {
        if(!initialized) {
          val params = httpclient.getParams
          params.setParameter("http.connection.timeout", connectionTimeout)
          params.setParameter("http.socket.timeout", dataTimeout)
          continueTimeout match {
            case Some(timeout) =>
              params.setParameter("http.protocol.expect-continue", true)
              params.setParameter("http.protocol.wait-for-continue", timeout)
            case None =>
              params.setParameter("http.protocol.expect-continue", false)
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

  private implicit def httpUriRequestResource[A <: HttpRequestBase] = new simplearm.Resource[A] {
    def close(a: A) {
      a.reset()
    }
  }

  private def send[A](req: HttpUriRequest, f: Response => A) = {
    val response = try {
      httpclient.execute(req)
    } catch {
      case e: UndeclaredThrowableException =>
        throw e.getCause
    }
    f(responsify(response))
  }

  def get(url: SimpleURL): Managed[Response] = new SimpleArm[Response] {
    def flatMap[A](f: Response => A): A = {
      init()
      using(new HttpGet(url.toString)) { get =>
        send(get, f)
      }
    }
  }

  def delete(url: SimpleURL): Managed[Response] = new SimpleArm[Response] {
    def flatMap[A](f: Response => A): A = {
      init()
      using(new HttpDelete(url.toString)) { delete =>
        send(delete, f)
      }
    }
  }

  def post[T: JsonCodec](url: SimpleURL, body: T): Managed[Response] =
    post(url, JValueEventIterator(JsonCodec[T].encode(body)))

  def post(url: SimpleURL, body: Iterator[JsonEvent]): Managed[Response] =
    streamBody(url, body, new HttpPost(_))

  def put[T: JsonCodec](url: SimpleURL, body: T): Managed[Response] =
    put(url, JValueEventIterator(JsonCodec[T].encode(body)))

  def put(url: SimpleURL, body: Iterator[JsonEvent]): Managed[Response] =
    streamBody(url, body, new HttpPut(_))

  private def streamBody(url: SimpleURL, body: Iterator[JsonEvent], method: String => HttpEntityEnclosingRequestBase): Managed[Response] = new SimpleArm[Response] {
    def flatMap[A](f: Response => A): A = {
      init()
      val sendEntity = new InputStreamEntity(new ReaderInputStream(new JsonEventIteratorReader(body), StandardCharsets.UTF_8), -1, jsonContentType)
      sendEntity.setChunked(true)
      using(method(url.toString)) { op =>
        op.setEntity(sendEntity)
        send(op, f)
      }
    }
  }
}

object Blah extends App {
  for {
    cli <- managed(new HttpClientHttpClient(100000, 100000))
    compressed <- managed(new FileInputStream("/home/robertm/car_linej_lds_5_2011.small.mjson.gz"))
    uncompressed <- managed(new GZIPInputStream(compressed))
    reader <- managed(new InputStreamReader(uncompressed, StandardCharsets.UTF_8))
    resp <- cli.post(SimpleURL.http("localhost", port = 10000), new FusedBlockJsonEventIterator(reader))
  } {
    println(resp.responseInfo.resultCode)
    println(JsonReader.fromEvents(resp))
  }
}
