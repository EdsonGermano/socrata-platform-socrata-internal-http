scalaVersion := "2.10.2"

libraryDependencies ++= Seq(
  "org.apache.httpcomponents" % "httpclient" % "4.2.5",
  "com.rojoma" %% "simple-arm" % "1.1.10",
  "com.rojoma" %% "rojoma-json" % "2.4.0",
  "org.scalatest" %% "scalatest" % "1.9.1" % "test",
  "org.scalacheck" %% "scalacheck" % "1.10.1" % "test"
)
