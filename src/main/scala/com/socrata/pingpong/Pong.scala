package com.socrata.pingpong

import java.nio.channels.spi.SelectorProvider
import java.nio.{BufferOverflowException, ByteBuffer}
import scala.util.Random
import com.rojoma.simplearm.util._
import java.nio.channels.DatagramChannel
import java.net.SocketAddress
import java.io.IOException

class Pong(address: SocketAddress, send: Array[Byte], rng: Random = new Random) {
  // runs the mainloop, responding to every ping packet it receives.
  //
  // The packets that it sends consist of the following:
  //   [the contents of the received ping] [the contents of `send`]
  //
  // At any time, this thread may be interrupted.  That will also cause it to exit
  // via exception.
  def go() {
    val selectorProvider = SelectorProvider.provider
    using(selectorProvider.openDatagramChannel()) { socket =>
      socket.bind(address)

      val recvBuf = ByteBuffer.allocate(512)
      while(true) {
        recvBuf.clear()
        val respondTo = socket.receive(recvBuf)
        try {
          recvBuf.put(send).flip()
          socket.send(recvBuf, respondTo)
        } catch {
          case e: IOException => // ignore
          case e: BufferOverflowException => //ignore
        }
      }
    }
  }
}

object Pong extends App {
  import java.net._
  val pong = new Pong(new InetSocketAddress(InetAddress.getLocalHost, 1111), Array[Byte](1, 2, 3, 4))
  pong.go()
}
