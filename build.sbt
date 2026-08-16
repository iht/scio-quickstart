import sbt._
import Keys._

val scioVersion = "0.15.8"
val beamVersion = "2.74.0"
lazy val commonSettings = Def.settings(
  organization := "dev.herraiz",
  version := "0.1.0-SNAPSHOT",
  scalaVersion := "2.13.18",
  scalacOptions ++= Seq("-target:jvm-17",
                        "-deprecation",
                        "-feature",
                        "-unchecked",
                        "-Ymacro-annotations"),
  javacOptions ++= Seq("-source", "17", "-target", "17"),
  libraryDependencySchemes += "com.github.luben" % "zstd-jni" % VersionScheme.Always,
  dependencyOverrides ++= Seq(
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.4",
    "com.fasterxml.jackson.core" % "jackson-core" % "2.15.4",
    "com.fasterxml.jackson.core" % "jackson-annotations" % "2.15.4",
    "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % "2.22.2",
    "com.fasterxml.jackson.dataformat" % "jackson-dataformat-xml" % "2.15.4"
  )
)

lazy val root: Project = project
  .in(file("."))
  .settings(commonSettings)
  .settings(
    name := "scio-quickstart",
    description := "scio-quickstart",
    publish / skip := true,
    run / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat,
    libraryDependencies ++= Seq(
      "com.spotify" %% "scio-core" % scioVersion,
      "com.spotify" %% "scio-google-cloud-platform" % scioVersion,
      "com.spotify" %% "scio-extra" % scioVersion,
      "com.spotify" %% "scio-test" % scioVersion % Test,
      "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
      "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion,
      "org.slf4j" % "slf4j-simple" % "2.0.18"
//      "com.google.http-client" % "google-http-client-apache-v2" % "1.38.1"
    )
  )
  .enablePlugins(JavaAppPackaging)

lazy val repl: Project = project
  .in(file(".repl"))
  .settings(commonSettings)
  .settings(
    name := "repl",
    description := "Scio REPL for scio-scala-workshop",
    libraryDependencies ++= Seq(
      "com.spotify" %% "scio-repl" % scioVersion
    ),
    Compile / mainClass := Some("com.spotify.scio.repl.ScioShell"),
    publish / skip := true
  )
  .dependsOn(root)

resolvers += "confluent" at "https://packages.confluent.io/maven/"
