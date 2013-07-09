net.virtualvoid.sbt.graph.Plugin.graphSettings

scalaVersion := "2.10.2"

libraryDependencies ++= Seq(
  "org.apache.httpcomponents" % "httpclient" % "4.2.5" exclude ("commons-logging", "commons-logging"),
  "org.apache.httpcomponents" % "httpmime" % "4.2.5",
  "com.rojoma" %% "simple-arm" % "1.1.10",
  "com.rojoma" %% "rojoma-json" % "2.4.0",
  "org.scalatest" %% "scalatest" % "1.9.1" % "test",
  "org.scalacheck" %% "scalacheck" % "1.10.1" % "test",
  "org.slf4j" % "jcl-over-slf4j" % "1.7.5",
  "org.slf4j" % "slf4j-api" % "1.7.5",
  "org.slf4j" % "slf4j-simple" % "1.7.5"
)
