package com.socrata.simplehttp

import scala.annotation.tailrec
import java.io._
import java.nio.charset.{CodingErrorAction, Charset, CoderResult}
import java.nio.{ByteBuffer,CharBuffer}

class ReaderInputStream(reader: Reader, charset: Charset, val blockSize: Int = 1024) extends InputStream {
  require(blockSize > 0, "Non-positive block size")

  def this(reader: Reader, charset: String) = this(reader, Charset.forName(charset))
  private[this] val encoder = charset.newEncoder().onMalformedInput(CodingErrorAction.REPLACE).onUnmappableCharacter(CodingErrorAction.REPLACE).reset()

  private[this] val charBuffer = CharBuffer.allocate(blockSize)
  private[this] var isCharBufferReading = false
  private[this] val byteBuffer = ByteBuffer.allocate((blockSize * encoder.maxBytesPerChar.toDouble).toInt) // I hate you java I hate you forever
  private[this] var isByteBufferReading = false

  private[this] var totalCharsRead = 0L
  private[this] var sawReaderEOF = false
  private[this] var sawEncoderEOF = false
  private[this] var sawFlushEOF = false

  private def trace(s: Any*) = {} // println(s.mkString)
  private def trace(s: String) = {} // println(s)

  // returns true if targetByteBuffer could take more
  private def fill(targetByteBuffer: ByteBuffer): Boolean = {
    byteBufferReading()
    if(byteBuffer.hasRemaining) {
      val toCopy = Math.min(targetByteBuffer.remaining, byteBuffer.remaining)
      // No way to bulk copy a subset of a bytebuffer without making a temporary slice object?
      targetByteBuffer.put(byteBuffer.array, byteBuffer.position + byteBuffer.arrayOffset, toCopy)
      byteBuffer.position(byteBuffer.position + toCopy)
    }
    targetByteBuffer.hasRemaining
  }

  private def charBufferReading() {
    if(!isCharBufferReading) {
      charBuffer.flip()
      isCharBufferReading = true
    }
  }

  private def charBufferWriting() {
    if(isCharBufferReading) {
      charBuffer.compact()
      isCharBufferReading = false
    }
  }

  private def byteBufferReading() {
    if(!isByteBufferReading) {
      byteBuffer.flip()
      isByteBufferReading = true
    }
  }

  private def byteBufferWriting() {
    if(isByteBufferReading) {
      byteBuffer.compact()
      isByteBufferReading = false
    }
  }

  @tailrec
  private def readAsMuchAsPossible(targetByteBuffer: ByteBuffer) {
    if(!fill(targetByteBuffer)) return

    var doRead = true

    def doFlushEOF() {
      assert(!charBuffer.hasRemaining)

      byteBufferWriting()
      encoder.flush(byteBuffer) match {
        case CoderResult.UNDERFLOW =>
          trace("I have now seen the flush EOF")
          sawFlushEOF = true
        case CoderResult.OVERFLOW =>
          // pass
      }
      fill(targetByteBuffer)
    }

    def doEncoderEOF(): Boolean = {
      charBufferReading()
      byteBufferWriting()
      encoder.encode(charBuffer, byteBuffer, true) match {
        case CoderResult.UNDERFLOW =>
          assert(!charBuffer.hasRemaining)
          trace("I have now seen the encoder EOF")
          sawEncoderEOF = true
          doFlushEOF()
          true
        case CoderResult.OVERFLOW =>
          fill(targetByteBuffer)
          false
      }
    }

    if(!sawReaderEOF) {
      charBufferWriting()

      val preexisting = charBuffer.position
      trace("Reading into the char buffer after the first ", preexisting, " char(s)")
      val count = reader.read(charBuffer.array, preexisting + charBuffer.arrayOffset, charBuffer.capacity - preexisting)
      if(count == -1) {
        trace("I have now seen the reader EOF")
        sawReaderEOF = true
        if(preexisting == 0) {
          if(doEncoderEOF()) {
            return
          }
        }
      } else {
        trace("Read ", count, " char(s)")
        totalCharsRead += count
        charBuffer.position(preexisting + count)
      }
    } else if(!sawEncoderEOF) {
      if(doEncoderEOF()) doRead = false
    } else if(!sawFlushEOF) {
      doFlushEOF()
      doRead = false
    } else {
      trace("I have already seen all three EOFs")
      fill(targetByteBuffer)
      return
    }

    def encode() = {
      charBufferReading()
      byteBufferWriting()
      val res = encoder.encode(charBuffer, byteBuffer, sawReaderEOF)
      if(res == CoderResult.UNDERFLOW && sawReaderEOF) {
        assert(!charBuffer.hasRemaining)
        trace("I have now seen the encoder EOF")
        sawEncoderEOF = true
      }
      res
    }

    if(doRead) {
      trace("Encoding from the char buffer; there are ", charBuffer.remaining, " char(s) that must fit in ", byteBuffer.remaining, " byte(s)")
      encode() match {
        case CoderResult.UNDERFLOW =>
          trace("Underflow when encoding; there are ", charBuffer.remaining, " char(s) remaining in the source and ", byteBuffer.remaining, " byte(s) remaining in the target")
        case CoderResult.OVERFLOW =>
          trace("Overflow when encoding; there are ", charBuffer.remaining, " char(s) remaining in the source and ", byteBuffer.remaining, " byte(s) remaining in the target")
      }
    }

    readAsMuchAsPossible(targetByteBuffer)
  }

  override def read(bytes: Array[Byte], offset: Int, length: Int): Int = {
    if(length == 0) return 0

    val bb = ByteBuffer.wrap(bytes, offset, length)

    val remainingAtStart = bb.remaining
    readAsMuchAsPossible(bb)
    if(bb.remaining == remainingAtStart) {
      -1
    } else {
      val result = remainingAtStart - bb.remaining
      trace("Read ", result, " bytes")
      result
    }
  }

  def read() = {
    byteBufferReading()
    if(byteBuffer.hasRemaining) {
      byteBuffer.get() & 0xff
    } else {
      val b = new Array[Byte](1)
      if(read(b) == -1) -1
      else b(0)
    }
  }
}
