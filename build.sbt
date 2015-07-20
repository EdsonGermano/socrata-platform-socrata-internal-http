name := "socrata-internal-http"

libraryDependencies ++= Seq(
  "org.apache.httpcomponents" % "httpclient" % "4.2.5" exclude ("commons-logging", "commons-logging"),
  "org.apache.httpcomponents" % "httpmime" % "4.2.5",
  "com.rojoma" %% "simple-arm" % "1.1.10",
  "com.rojoma" %% "rojoma-json-v3" % "3.3.0",
  "org.scalacheck" %% "scalacheck" % "1.12.4" % "test",
  "org.slf4j" % "jcl-over-slf4j" % "1.7.5",
  "org.slf4j" % "slf4j-api" % "1.7.5",
  "org.slf4j" % "slf4j-simple" % "1.7.5" % "test"
)

// TODO: enable code coverage build failures
scoverage.ScoverageSbtPlugin.ScoverageKeys.coverageFailOnMinimum := false
// TODO: enable scalastyle build failures
com.socrata.sbtplugins.StylePlugin.StyleKeys.styleFailOnError in Compile := false
