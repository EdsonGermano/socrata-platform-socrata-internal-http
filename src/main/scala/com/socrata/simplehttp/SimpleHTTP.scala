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
  def unparsableContentType() = ???
  def responseNotJson() = ???
  def illegalCharsetName() = ???
  def unsupportedCharset() = ???

  private def responsify(response: HttpResponse): Iterator[JsonEvent] with ResponseInfoProvider = {
    val entity = response.getEntity
    val contentType = entity.getContentType
    if(contentType == null) noContentTypeInResponse()

    val charset = try {
      val mimeType = new MimeType(contentType.getValue)
      if(mimeType.getBaseType != jsonContentTypeBase) responseNotJson()
      Option(mimeType.getParameter("charset")).map(Charset.forName).getOrElse(StandardCharsets.ISO_8859_1)
    } catch {
      case _: MimeTypeParseException =>
        unparsableContentType()
      case _: IllegalCharsetNameException =>
        illegalCharsetName()
      case _: UnsupportedCharsetException =>
        unsupportedCharset()
    }

    val reader = new InputStreamReader(entity.getContent, charset)
    new FusedBlockJsonEventIterator(reader) with ResponseInfoProvider with ResponseInfo {
      def responseInfo = this
      val resultCode = response.getStatusLine.getStatusCode
    }
  }

  def get(url: SimpleURL): Managed[Response] = new SimpleArm[Response] {
    def flatMap[A](f: Response => A): A = {
      init()
      val get = new HttpGet(url.toString)
      try {
        val response = httpclient.execute(get)
        f(responsify(response))
      } finally {
        get.reset()
      }
    }
  }

  def delete(url: SimpleURL): Managed[Response] = new SimpleArm[Response] {
    def flatMap[A](f: Response => A): A = {
      init()
      val delete = new HttpDelete(url.toString)
      try {
        val response = httpclient.execute(delete)
        f(responsify(response))
      } finally {
        delete.reset()
      }
    }
  }

  def post[T: JsonCodec](url: SimpleURL, body: T): Managed[Response] =
    codecBody(url, body, new HttpPost(_))

  def post(url: SimpleURL, body: Iterator[JsonEvent]): Managed[Response] =
    streamBody(url, body, new HttpPost(_))

  def put[T: JsonCodec](url: SimpleURL, body: T): Managed[Response] =
    codecBody(url, body, new HttpPut(_))

  def put(url: SimpleURL, body: Iterator[JsonEvent]): Managed[Response] =
    streamBody(url, body, new HttpPut(_))

  private def codecBody[T : JsonCodec](url: SimpleURL, body: T, method: String => HttpEntityEnclosingRequestBase): Managed[Response] = new SimpleArm[Response] {
    def flatMap[A](f: Response => A): A = {
      init()
      val post = method(url.toString)
      try {
        val sendEntity = new StringEntity(CompactJsonWriter.toString(JsonCodec[T].encode(body)), jsonContentType)
        withBody(url, sendEntity, method, f)
      } finally {
        post.reset()
      }
    }
  }

  private def streamBody(url: SimpleURL, body: Iterator[JsonEvent], method: String => HttpEntityEnclosingRequestBase): Managed[Response] = new SimpleArm[Response] {
    def flatMap[A](f: Response => A): A = {
      val sendEntity = new InputStreamEntity(new ReaderInputStream(new JsonEventIteratorReader(body), StandardCharsets.UTF_8), -1, jsonContentType)
      sendEntity.setChunked(true)
      withBody(url, sendEntity, method, f)
    }
  }

  private def withBody[A](url: SimpleURL, sendEntity: HttpEntity, method: String => HttpEntityEnclosingRequestBase, f: Response => A): A = {
    init()
    val post = method(url.toString)
    try {
      post.setEntity(sendEntity)
      val response = httpclient.execute(post)
      f(responsify(response))
    } finally {
      post.reset()
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
