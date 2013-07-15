package com.socrata.internal.http.pingpong

import scala.concurrent.duration._
import java.net.SocketAddress
import java.nio.channels.spi.SelectorProvider
import java.nio.ByteBuffer
import scala.util.Random
import com.rojoma.simplearm.util._
import java.nio.channels.{Selector, SelectionKey, DatagramChannel}
import scala.annotation.tailrec
import java.io.IOException

case class PingSpec(address: SocketAddress, hostFingerprint: Array[Byte], interval: FiniteDuration, missable: Int, rng: Random = new Random) {
  require(missable >= 0, "missable")
}

class Ping(pingSpec: PingSpec) {
  import Ping._
  import pingSpec._

  private val intervalNS = interval.toNanos

  // runs the mainloop, sending a packet every "interval" seconds, until "missable"
  // have not been returned in a row.  Then it exits.
  //
  // The packets that it sends consist of the following:
  //   [4 byte counter] [16 byte random]
  // What counts as a valid response is the same thing, only suffixed
  // with the contents of `receive`.
  //
  // At any time, this thread may be interrupted.  That will also cause it to exit
  // via exception.
  def go() {
    val selectorProvider = SelectorProvider.provider
    for {
      selector <- managed(selectorProvider.openSelector())
      socket <- managed(selectorProvider.openDatagramChannel())
    } {
      socket.configureBlocking(false)
      socket.connect(address)

      new Op(selector, socket).go()
    }
  }

  private class Op(selector: Selector, socket: DatagramChannel) {
    socket.register(selector, SelectionKey.OP_READ)

    val me = new Array[Byte](16)
    rng.nextBytes(me)

    val txPacket = locally {
      val packet = ByteBuffer.allocate(20)
      packet.putInt(0)
      packet.put(me)
      packet.flip()
      packet
    }

    val rxPacketBuffer =
      ByteBuffer.allocate(txPacket.remaining + hostFingerprint.length + 1)

    def go() {
      var counter = 0
      var missed = 0

      while(true) {
        counter += 1

        fillInTxPacket(counter)

        val deadline = System.nanoTime + intervalNS

        try {
          log.trace("Ping?")
          socket.write(txPacket)

          if(!awaitGoodPacket(deadline, counter)) {
            missed += 1
            log.debug("No response; missed {} packet(s) in a row", missed)
            if(missed >= missable) return
          } else {
            log.trace("Pong!")
            missed = 0
            sleepTo(deadline)
          }
        } catch {
          case e: IOException =>
            missed += 1
            log.debug("IO exception while pinging; counting this as missed ({} in a row now)", missed)
            if(missed >= missable) return
            sleepTo(deadline)
        }
      }
    }

    def sleepTo(deadline: Long) {
      val ms = (deadline - System.nanoTime()) / 1000000
      if(ms > 0) Thread.sleep(ms)
    }

    private def fillInTxPacket(counter: Int) {
      txPacket.clear()
      txPacket.putInt(0, counter)
    }

    @tailrec
    private def awaitGoodPacket(deadline: Long, counter: Int): Boolean = {
      rxPacketBuffer.clear()
      if(socket.read(rxPacketBuffer) == 0) { // no packet at all
        if(!awaitReadable(deadline)) false
        else awaitGoodPacket(deadline, counter)
      } else if(!isGoodResponse(counter)) {
        awaitGoodPacket(deadline, counter)
      } else { // ok great
        true
      }
    }

    def awaitReadable(deadline: Long): Boolean = {
      val duration = Math.max(1, (deadline - System.nanoTime) / 1000000)
      if(selector.select(duration) != 0) {
        selector.selectedKeys().clear()
        true
      } else {
        false
      }
    }

    def isGoodResponse(counter: Int): Boolean = {
      rxPacketBuffer.flip()
      if(rxPacketBuffer.remaining != rxPacketBuffer.capacity - 1) return false
      if(rxPacketBuffer.getInt() != counter) return false

      def checkBytes(bs: Array[Byte]) = {
        var i = 0
        while(i != bs.length && rxPacketBuffer.get() == bs(i)) {
          i += 1
        }
        i == bs.length
      }

      if(!checkBytes(me)) return false
      if(!checkBytes(hostFingerprint)) return false

      true
    }
  }
}

object Ping {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[Ping])
}
