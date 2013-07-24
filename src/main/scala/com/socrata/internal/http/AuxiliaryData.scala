package com.socrata.internal.http

import com.socrata.internal.http.pingpong.PingInfo

/** A class for storing in a curator service advertisement's payload field.
  * All fields of this class must be (de)serializable by Jackson. */
class AuxiliaryData(var pingInfo: Option[PingInfo]) {
  @deprecated(message = "This constructor is for Jackson's use, not yours", since = "forever")
  def this() = this(None)

  // can't use @BeanProperty because of the Option
  def getPingInfo = pingInfo.orNull
  def setPingInfo(pi: PingInfo) { pingInfo = Option(pi) }
}
