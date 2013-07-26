package com.socrata.internal.http

import java.io.{InputStreamReader, Reader, InputStream, Closeable}
import java.nio.charset.{UnsupportedCharsetException, IllegalCharsetNameException, StandardCharsets, Charset}
import javax.activation.{MimeTypeParseException, MimeType}

import com.rojoma.json.io.{JsonReader, FusedBlockJsonEventIterator, JsonEvent}
import com.rojoma.simplearm.{SimpleArm, Managed}
import org.apache.http.entity.ContentType

import com.socrata.internal.http.util.{AcknowledgeableInputStream, Acknowledgeable}
import com.socrata.internal.http.exceptions._
import com.socrata.internal.http.pingpong.PingInfo
import com.rojoma.json.ast.JValue
import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.util.JsonArrayIterator

trait Response extends ResponseInfo {
  def asInputStream(maximumSizeBetweenAcks: Long = Long.MaxValue): InputStream with Acknowledgeable
  def asReader(maximumSizeBetweenAcks: Long = Long.MaxValue): Reader with Acknowledgeable
  def isJson: Boolean
  def asJsonEvents(maximumSizeBetweenAcks: Long = Long.MaxValue): Iterator[JsonEvent] with Acknowledgeable
  def asJValue(approximateMaximumSize: Long = Long.MaxValue): JValue
  def asValue[T : JsonCodec](approximateMaximumSize: Long = Long.MaxValue): Option[T]
  def asArray[T : JsonCodec](approximateMaximumElementSize: Long = Long.MaxValue): Iterator[T]
}

trait HttpClient extends Closeable {
  import HttpClient._

  type RawResponse = (ResponseInfo, InputStream)

  protected def connectTimeout() = throw new ConnectTimeout
  protected def receiveTimeout() = throw new ReceiveTimeout
  protected def connectFailed(cause: java.net.ConnectException) = throw new ConnectFailed(cause)
  protected def livenessCheckFailed() = throw new LivenessCheckFailed
  protected def fullTimeout() = throw new FullTimeout
  protected def multipleContentTypesInResponse() = throw new MultipleContentTypesInResponse
  protected def unparsableContentType(contentType: String) = throw new UnparsableContentType(contentType)
  protected def responseNotJson(mimeType: Option[MimeType]) = throw new UnexpectedContentType(got = mimeType, expected = jsonContentTypeBase)
  protected def illegalCharsetName(charsetName: String) = throw new IllegalCharsetName(charsetName)
  protected def unsupportedCharset(charsetName: String) = throw new UnsupportedCharset(charsetName)
  protected def noBodyInResponse() = throw new NoBodyInResponse

  private def contentType(responseInfo: ResponseInfo): Option[MimeType] = responseInfo.headers("content-type") match {
    case Array(ct) =>
      try {
        Some(new MimeType(ct))
      } catch {
        case _: MimeTypeParseException =>
          unparsableContentType(ct)
      }
    case Array() =>
      None
    case _ =>
      multipleContentTypesInResponse()
  }

  private def charset(contentType: Option[MimeType]): Charset = contentType match {
    case Some(mimeType) =>
      try {
        Option(mimeType.getParameter("charset")).map(Charset.forName).getOrElse(StandardCharsets.ISO_8859_1)
      } catch {
        case e: IllegalCharsetNameException =>
          illegalCharsetName(e.getCharsetName)
        case e: UnsupportedCharsetException =>
          unsupportedCharset(e.getCharsetName)
      }
    case None =>
      StandardCharsets.ISO_8859_1
  }

  /**
   * Executes the request.
   *
   * @return an `InputStream` with attached HTTP response info.
   */
  def executeRaw(req: SimpleHttpRequest, ping: Option[PingInfo] = None): Managed[RawResponse]

  def execute(req: SimpleHttpRequest, ping: Option[PingInfo] = None): Managed[Response] =
    new SimpleArm[Response] {
      def flatMap[A](f: Response => A): A =
        for(rawResponse <- executeRaw(req, ping)) yield {
          val cooked = new Response {
            val (responseInfo, rawInputStream) = rawResponse
            lazy val ct = contentType(responseInfo)
            lazy val cs = charset(ct)
            var created = false

            def asInputStream(maximumSizeBetweenAcks: Long = Long.MaxValue): InputStream with Acknowledgeable = {
              if(created) throw new IllegalStateException("Already got the response body")
              created = true
              new AcknowledgeableInputStream(rawInputStream, maximumSizeBetweenAcks)
            }

            private def asReader(charset: Charset, maximumSizeBetweenAcks: Long) = {
              val stream = asInputStream(maximumSizeBetweenAcks)
              new InputStreamReader(stream, charset) with Acknowledgeable {
                def acknowledge() = stream.acknowledge()
              }
            }

            def asReader(maximumSizeBetweenAcks: Long = Long.MaxValue): Reader with Acknowledgeable =
              asReader(cs, maximumSizeBetweenAcks)

            lazy val isJson: Boolean =
              ct match {
                case Some(mime) => mime.getBaseType == jsonContentTypeBase
                case None => false
              }

            def asJsonEvents(maximumSizeBetweenAcks: Long = Long.MaxValue): Iterator[JsonEvent] with Acknowledgeable = {
              if(!isJson) throw responseNotJson(ct)
              val reader = asReader(maximumSizeBetweenAcks)
              new FusedBlockJsonEventIterator(reader) with Acknowledgeable {
                def acknowledge() = reader.acknowledge()
              }
            }

            def asJValue(approximateMaximumSize: Long): JValue =
              JsonReader.fromEvents(asJsonEvents(approximateMaximumSize))

            def asValue[T: JsonCodec](approximateMaximumSize: Long): Option[T] =
              JsonCodec[T].decode(asJValue(approximateMaximumSize))

            def asArray[T: JsonCodec](approximateMaximumElementSize: Long): Iterator[T] =
              new Iterator[T] {
                val events = asJsonEvents(approximateMaximumElementSize)
                val rawIt = JsonArrayIterator[T](events)

                def hasNext = {
                  events.acknowledge()
                  rawIt.hasNext
                }

                def next() = {
                  events.acknowledge()
                  rawIt.next()
                }
              }

            def resultCode: Int = responseInfo.resultCode
            def headers(name: String): Array[String] = responseInfo.headers(name)
            def headerNames: Set[String] = responseInfo.headerNames
          }
          f(cooked)
        }
    }
}

object HttpClient {
  val jsonContentTypeBase = "application/json"
  val jsonContentType = ContentType.create(jsonContentTypeBase, StandardCharsets.UTF_8)
  val formContentTypeBase = "application/x-www-form-urlencoded"
  val formContentType = ContentType.create(formContentTypeBase)
}
