package com.socrata.internal.http.util

import scala.collection.JavaConverters._
import java.io.Closeable
import java.util.concurrent.{Executor, TimeUnit}

import TimeoutManager._

class TimeoutManager(executor: Executor) extends Closeable {
  private sealed trait PendingJob
  private case class CancelJob(job: Job) extends PendingJob
  private case class Job(onTimeout: () => Any, deadline: Long) extends IntrusivePriorityQueueNode with PendingJob with Closeable {
    priority = deadline
    def close() {
      pendingJobs.add(CancelJob(this))
    }
  }
  private object PoisonPill extends PendingJob

  private val worker = new Thread {
    override def run() {
      mainloop()
    }
  }
  private val pendingJobs = new java.util.concurrent.LinkedBlockingQueue[PendingJob]
  private val jobs = new IntrusivePriorityQueue[Job]

  def start() {
    worker.start()
  }

  def close() {
    pendingJobs.add(PoisonPill)
    worker.join()
  }

  private def mainloop() {
    while(true) {
      val now = System.currentTimeMillis()

      while(jobs.nonEmpty && jobs.head.deadline <= now) runJob(jobs.pop())

      val newJobs = new java.util.ArrayList[PendingJob]
      if(jobs.nonEmpty) {
        pendingJobs.drainTo(newJobs)
        if(newJobs.isEmpty) {
          val job = pendingJobs.poll(jobs.head.deadline - now, TimeUnit.MILLISECONDS)
          if(job != null) pendingJobs.add(job)
        }
      } else {
        pendingJobs.drainTo(newJobs)
        if(newJobs.isEmpty) {
          pendingJobs.add(pendingJobs.take())
        }
      }

      newJobs.asScala.foreach {
        case j: Job =>
          jobs.add(j)
        case CancelJob(job) =>
          jobs.remove(job)
        case PoisonPill =>
          if(jobs.nonEmpty) log.warn("Shutting down with " + jobs.size + " timeout jobs remaining")
          return
        case null => // timed out while waiting for the front job
          runJob(jobs.pop())
      }
    }
  }

  def addJob(timeout: Int)(onTimeout: => Any): Closeable = {
    val result = Job(() => onTimeout, System.currentTimeMillis() + timeout)
    pendingJobs.add(result)
    result
  }

  private def runJob(job: Job) {
    executor.execute(new Runnable {
      def run() { job.onTimeout() }
    })
  }
}

object TimeoutManager {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[TimeoutManager])
}
