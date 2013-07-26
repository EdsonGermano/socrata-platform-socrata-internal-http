package com.socrata.internal.http

import java.io.{InputStreamReader, Reader, InputStream, Closeable}
import java.nio.charset.{UnsupportedCharsetException, IllegalCharsetNameException, StandardCharsets, Charset}
import javax.activation.{MimeTypeParseException, MimeType}

import com.rojoma.json.io.{JsonReader, FusedBlockJsonEventIterator, JsonEvent}
import com.rojoma.simplearm.{SimpleArm, Managed}
import org.apache.http.entity.ContentType

import com.socrata.internal.http.util.Acknowledgeable
import com.socrata.internal.http.exceptions._
import com.socrata.internal.http.pingpong.PingInfo
import com.rojoma.json.ast.JValue
import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.util.JsonArrayIterator

trait HttpClient extends Closeable {
  import HttpClient._

  type RawResponse = InputStream with Acknowledgeable with ResponseInfoProvider
  type RawJsonResponse = Reader with Acknowledgeable with ResponseInfoProvider
  type JsonResponse = Iterator[JsonEvent] with Acknowledgeable with ResponseInfoProvider
  type ArrayResponse[T] = Iterator[T] with ResponseInfoProvider

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
   * @return an [[com.socrata.internal.http.util.Acknowledgeable]] `Reader` with attached HTTP response info.
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
   * @return an [[com.socrata.internal.http.util.Acknowledgeable]] `Iterator[JsonEvent]` with attached HTTP response info.
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

  def executeForArray[T : JsonCodec](req: SimpleHttpRequest, ping: Option[PingInfo], approximateMaximumSizePerElement: Long = Long.MaxValue): Managed[ArrayResponse[T]] = {
    new SimpleArm[ArrayResponse[T]] {
      def flatMap[A](f: ArrayResponse[T] => A): A =
        for(events <- executeForJson(req, ping, approximateMaximumSizePerElement)) yield {
          val it = new Iterator[T] with ResponseInfoProvider {
            val rawIt = JsonArrayIterator[T](events)

            def hasNext = {
              events.acknowledge()
              rawIt.hasNext
            }

            def next() = {
              events.acknowledge()
              rawIt.next()
            }

            val responseInfo = events.responseInfo
          }
          f(it)
        }
    }
  }

  def executeForJValue(req: SimpleHttpRequest, ping: Option[PingInfo], approximateMaximumSize: Long = Long.MaxValue): (ResponseInfo, JValue) =
    for(response <- executeForJson(req, ping, maximumSizeBetweenAcks = approximateMaximumSize)) yield
      (response.responseInfo, JsonReader.fromEvents(response))

  def executeForJsonable[T : JsonCodec](req: SimpleHttpRequest, ping: Option[PingInfo], approximateMaximumSize: Long = Long.MaxValue): (ResponseInfo, Option[T]) = {
    val (info, jvalue) = executeForJValue(req, ping, approximateMaximumSize)
    (info, JsonCodec[T].decode(jvalue))
  }
}

object HttpClient {
  val jsonContentTypeBase = "application/json"
  val jsonContentType = ContentType.create(jsonContentTypeBase, StandardCharsets.UTF_8)
  val formContentTypeBase = "application/x-www-form-urlencoded"
  val formContentType = ContentType.create(formContentTypeBase)
}
