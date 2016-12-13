name := "googlecloud-shapeless"

organization := "com.emarsys"

version := "0.0.1.7"

scalaVersion := "2.11.8"

libraryDependencies ++= {
  val scalaTestV  = "3.0.0"
  Seq(
    "com.chuusai"               %% "shapeless"                          % "2.3.2",
    "org.scalacheck"            %% "scalacheck"                         % "1.13.3" % "test",
    "org.scalatest"             %% "scalatest"                          % scalaTestV % "test",
    "joda-time"                 %  "joda-time"                          % "2.9.1",
    "org.joda"                  %  "joda-convert"                       % "1.8",
    "org.typelevel"             %% "cats"                               % "0.4.1",
    "com.google.cloud"          %  "google-cloud"                       % "0.3.0",
    "com.google.cloud.dataflow" %  "google-cloud-dataflow-java-sdk-all" % "1.7.0",
    "com.twitter"               %% "algebird-core"                      % "0.11.0",
    "com.twitter"               %% "algebird-util"                      % "0.11.0",
    "com.twitter"               %% "algebird-bijection"                 % "0.11.0",
    "com.twitter"               %% "algebird-test"                      % "0.11.0" % "test",
    "com.spotify"               %% "scio-core"                          % "0.2.5",
    "com.spotify"               %% "scio-test"                          % "0.2.5" % "test"
  )
}

addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)

publishTo := Some(Resolver.file("releases", new File("releases")))
