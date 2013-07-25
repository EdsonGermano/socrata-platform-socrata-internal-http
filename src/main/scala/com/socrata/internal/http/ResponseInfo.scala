package com.socrata.internal.http

trait ResponseInfo {
  def resultCode: Int
  def headers(name: String): Array[String] // this will return an empty array if the header does not exist
  def headerNames: Set[String] // All the header names, canonicalized to lower-case
}

trait ResponseInfoProvider {
  def responseInfo: ResponseInfo
}
