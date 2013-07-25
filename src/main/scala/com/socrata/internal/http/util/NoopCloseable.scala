package com.socrata.internal.http.util

import java.io.Closeable

object NoopCloseable extends Closeable {
  def close() {}
}
