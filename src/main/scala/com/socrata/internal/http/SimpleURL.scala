package com.socrata.internal.http

case class SimpleURL(scheme: String, host: String, port: Int, path: Iterable[String], query: Iterable[(String, String)] = Nil) {
  import SimpleURL._

  override lazy val toString = {
    val queryless = scheme + "://" + host + ":" + port + "/" + path.iterator.map(encP).mkString("/")
    if(query.nonEmpty) {
      queryless + "?" + query.iterator.map{case (k,v) => encQ(k) + "=" + encQ(v)}.mkString("&")
    } else {
      queryless
    }
  }

  def asURL = new java.net.URL(toString)
}

object SimpleURL {
  def http(host: String, port: Int = 80, path: Iterable[String] = Nil, query: Iterable[(String, String)] = Nil) =
    SimpleURL("http", host, port, path, query)

  def https(host: String, port: Int = 443, path: Iterable[String] = Nil, query: Iterable[(String, String)] = Nil) =
    SimpleURL("https", host, port, path, query)

  private[this] val hexDigit = "0123456789ABCDEF".toCharArray
  private[this] val encPB = locally {
    val x = new Array[Boolean](256)
    for(c <- 'a' to 'z') x(c.toInt) = true
    for(c <- 'A' to 'Z') x(c.toInt) = true
    for(c <- '0' to '9') x(c.toInt) = true
    for(c <- ":@-._~!$&'()*+,;=") x(c.toInt) = true
    x.map(!_)
  }

  private def encP(s: String) = {
    val bs = s.getBytes("UTF-8")
    val sb = new java.lang.StringBuilder
    var i = 0
    while(i != bs.length) {
      val b = bs(i & 0xff)
      if(encPB(b)) {
        sb.append('%').append(hexDigit(b >>> 4)).append(hexDigit(b & 0xf))
      } else {
        sb.append(b.toChar)
      }
      i += 1
    }
    sb.toString
  }

  private def encQ(s: String) = java.net.URLEncoder.encode(s, "UTF-8")
}
