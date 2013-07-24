package com.socrata.internal.http

import java.io.InputStream
import java.net.SocketTimeoutException

class ReceiveTimeoutCatchingInputStream(underlying: InputStream, onReceiveTimeout: () => Nothing) extends InputStream {
  @inline
  private def maybeReceiveTimeout[T](f: => T): T =
    try {
      f
    } catch {
      case e: SocketTimeoutException =>
        onReceiveTimeout()
    }

  def read(): Int =
    maybeReceiveTimeout(underlying.read())

  override def read(bs: Array[Byte]): Int =
    maybeReceiveTimeout(underlying.read(bs))

  override def read(bs: Array[Byte], off: Int, len: Int): Int =
    maybeReceiveTimeout(underlying.read(bs, off, len))

  override def skip(n: Long): Long =
    maybeReceiveTimeout(underlying.skip(n))

  override def available: Int =
    maybeReceiveTimeout(underlying.available)

  override def mark(n: Int) =
    maybeReceiveTimeout(underlying.mark(n))

  override def reset() =
    maybeReceiveTimeout(underlying.reset())

  override def close() =
    maybeReceiveTimeout(underlying.close())
}
