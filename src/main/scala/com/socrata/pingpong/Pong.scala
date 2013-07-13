package com.socrata.pingpong

import java.nio.channels.spi.SelectorProvider
import java.nio.{BufferOverflowException, ByteBuffer}
import scala.util.Random
import com.rojoma.simplearm.util._
import java.nio.channels.{ClosedByInterruptException, DatagramChannel}
import java.net.{InetSocketAddress, SocketAddress}
import java.io.IOException
import java.util.concurrent.CountDownLatch

class Pong(address: InetSocketAddress, send: Array[Byte], rng: Random = new Random) {
  private val log = org.slf4j.LoggerFactory.getLogger(classOf[Pong])

  // runs the mainloop, responding to every ping packet it receives.
  //
  // The packets that it sends consist of the following:
  //   [the contents of the received ping] [the contents of `send`]
  //
  // At any time, this thread may be interrupted.  This will cause it to exit.
  def go() {
    try {
      val selectorProvider = SelectorProvider.provider
      using(selectorProvider.openDatagramChannel()) { socket =>
        socket.bind(address)

        port = socket.getLocalAddress.asInstanceOf[InetSocketAddress].getPort
        started.countDown()

        val recvBuf = ByteBuffer.allocate(512)
        while(true) {
          recvBuf.clear()
          val respondTo = socket.receive(recvBuf)
          try {
            log.trace("Ping from {}", respondTo)
            recvBuf.put(send).flip()
            socket.send(recvBuf, respondTo)
            log.trace("Pong!")
          } catch {
            case e: IOException =>
              log.trace("IO exception sending to the peer; ignoring")
            case e: BufferOverflowException =>
              log.trace("packet too large to add reply and still fit in 512 bytes; ignoring")
          }
        }
      }
    } catch {
      case _: InterruptedException | _: ClosedByInterruptException =>
        // pass
      case e: Throwable =>
        problem = e
        started.countDown()
        throw e
    }
  }

  private val started = new CountDownLatch(1)
  @volatile private var port = 0
  @volatile private var problem: Throwable = null

  def awaitStart(): Int = {
    started.await()
    if(problem != null) throw new Exception("Exception on pong thread", problem)
    port
  }
}
