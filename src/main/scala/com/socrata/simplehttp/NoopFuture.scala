package com.socrata.simplehttp

import java.util.concurrent.{TimeUnit, Future}

class NoopFuture[T](value: T) extends Future[T] {
  def cancel(mayInterruptIfRunning: Boolean): Boolean = false

  def isCancelled: Boolean = false

  def isDone: Boolean = true

  def get(): T = value

  def get(timeout: Long, unit: TimeUnit): T = value
}
