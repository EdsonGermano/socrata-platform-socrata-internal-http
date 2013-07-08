package com.socrata.simplehttp

import scala.collection.JavaConverters._
import org.scalatest.FunSuite
import scala.util.Random
import java.io._
import org.scalatest.matchers.MustMatchers
import org.scalatest.exceptions.ModifiableMessage
import java.nio.charset.Charset

class ReaderInputStreamTest extends FunSuite with MustMatchers {
  def range(min: Int, max: Int)(implicit rng: Random): Int =
    min + rng.nextInt(1 + max - min)

  val allCharsets = Charset.availableCharsets().values.asScala.filter(_.canEncode).toArray

  def generateValidString(minLen: Int, maxLen: Int, fracSurrogatePair: Double)(implicit rng: Random): String = {
    val len = range(minLen, maxLen)
    val sb = new StringBuilder(len)
    for(i <- 0 until len) {
      if(rng.nextDouble < fracSurrogatePair) {
        val high = range(Character.MIN_HIGH_SURROGATE, Character.MAX_HIGH_SURROGATE)
        val low = range(Character.MIN_LOW_SURROGATE, Character.MAX_LOW_SURROGATE)
        sb.append(high.toChar).append(low.toChar)
      } else {
        var c: Char = '\0'
        do {
          c = range(0, Char.MaxValue).toChar
        } while(Character.isSurrogate(c))
        sb.append(c)
      }
    }
    sb.toString
  }

  def generateInvalidString(minLen: Int, maxLen: Int, fracReversed: Double, fracStrayHigh: Double, fracStrayLow: Double)(implicit rng: Random): String = {
    val len = range(minLen, maxLen)
    val sb = new StringBuilder(len)
    for(i <- 0 until len) {
      val p = rng.nextDouble()
      if(p < fracReversed) {
        val high = range(Character.MIN_HIGH_SURROGATE, Character.MAX_HIGH_SURROGATE)
        val low = range(Character.MIN_LOW_SURROGATE, Character.MAX_LOW_SURROGATE)
        sb.append(low.toChar).append(high.toChar)
      } else if(p < fracReversed + fracStrayHigh) {
        val high = range(Character.MIN_HIGH_SURROGATE, Character.MAX_HIGH_SURROGATE)
        sb.append(high.toChar)
      } else if(p < fracReversed + fracStrayHigh + fracStrayLow) {
        val low = range(Character.MIN_LOW_SURROGATE, Character.MAX_LOW_SURROGATE)
        sb.append(low.toChar)
      } else {
        var c: Char = '\0'
        do {
          c = range(0, Char.MaxValue).toChar
        } while(Character.isSurrogate(c))
        sb.append(c)
      }
    }
    sb.toString
  }

  private class ShortReader(r: Reader)(implicit rng: Random) extends Reader {
    def read(cbuf: Array[Char], off: Int, len: Int): Int = {
      if(len == 0) return 0
      r.read(cbuf, off, range(1, len))
    }

    def close() { r.close() }
  }

  private def copyTo(from: InputStream, to: OutputStream) {
    val buf = new Array[Byte](1024)
    def loop() {
      from.read(buf) match {
        case -1 => // done
        case n =>
          to.write(buf, 0, n)
          loop()
      }
    }
    loop()
  }

  class SpecificSeedProvider(l: Long) {
    def nextLong() = l
  }

  test("Reading from a ReaderInputStream gives the same results as just calling getBytes on a valid string") {
    val seedProvider = new Random
    for(i <- 1 to 1000) {
      val seed = seedProvider.nextLong()
      doValidTest(seed)
    }
  }

  test("Reading from a ReaderInputStream gives the same results as just calling getBytes on an invalid string") {
    val seedProvider = new Random
    for(i <- 1 to 1000) {
      val seed = seedProvider.nextLong()
      doInvalidTest(seed)
    }
  }

  private def doValidTest(seed: Long) {
    doTest(seed) { implicit rng =>
      generateValidString(0, 10000, 0.25)
    }
  }

  private def doInvalidTest(seed: Long) {
    doTest(seed) { implicit rng =>
      generateInvalidString(0, 10000, 0.25, 0.25, 0.25)
    }
  }

  private def doTest(seed: Long)(genS: Random => String) {
    withClue(seed) {
      try {
        implicit val rng = new Random(seed)

        val s: String = genS(rng)

        val cs = allCharsets(range(0, allCharsets.length - 1))
        val bs = new ByteArrayOutputStream
        val ris = new ReaderInputStream(new ShortReader(new StringReader(s)), cs, range(1, 65535))
        copyTo(ris, bs)

        val fromReader = bs.toByteArray
        val fromString = s.getBytes(cs)

        fromReader must equal (fromString)
      } catch {
        case e: Throwable if !e.isInstanceOf[ModifiableMessage[_]] =>
          fail(e)
      }
    }
  }
}
