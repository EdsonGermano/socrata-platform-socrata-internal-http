package com.socrata.internal.http.exceptions

class HttpClientException extends Exception

class HttpClientTimeoutException extends HttpClientException
class ConnectTimeout extends HttpClientTimeoutException
class ReceiveTimeout extends HttpClientTimeoutException
class FullTimeout extends HttpClientTimeoutException

class LivenessCheckFailed extends HttpClientException
class ConnectFailed extends HttpClientException // failed for not-timeout reasons
class NoBodyInResponse extends HttpClientException

class ContentTypeException extends HttpClientException
class NoContentTypeInResponse extends ContentTypeException
class MultipleContentTypesInResponse extends ContentTypeException
class UnparsableContentType(val contentType: String) extends ContentTypeException
class UnexpectedContentType(val got: String, val expected: String) extends ContentTypeException
class IllegalCharsetName(val charsetName: String) extends ContentTypeException
class UnsupportedCharset(val charsetName: String) extends ContentTypeException
