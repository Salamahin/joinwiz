name := "joinwiz"

lazy val scala212 = "2.12.14"
lazy val scala211 = "2.11.12"
lazy val supportedScalaVersions = List(scala212, scala211)

lazy val commonSettings = Seq(
  scalacOptions ++= Seq(
    "-encoding",
    "utf8",
    "-Xfatal-warnings",
    "-deprecation",
    "-language:postfixOps",
    "-language:higherKinds",
    "-language:implicitConversions",
    "-feature",
    "-language:existentials"
  )
)

val sparkBinaries = Def.setting {
  val sparkV = Map(
    "2.11" -> "2.3.1",
    "2.12" -> "2.4.8"
  )

  "org.apache.spark"   %% "spark-core" % sparkV(scalaBinaryVersion.value) ::
    "org.apache.spark" %% "spark-sql"  % sparkV(scalaBinaryVersion.value) ::
    Nil
}

val scalaTest = Def.setting { "org.scalatest" %% "scalatest" % "3.1.0" % Test }

lazy val joinwiz_macro = project
  .settings(crossScalaVersions := supportedScalaVersions)
  .settings(commonSettings: _*)
  .settings(libraryDependencies ++= "org.scala-lang" % "scala-reflect" % scalaVersion.value :: sparkBinaries.value)

lazy val joinwiz_core = project
  .dependsOn(joinwiz_macro)
  .settings(crossScalaVersions := supportedScalaVersions)
  .settings(commonSettings: _*)
  .settings(libraryDependencies ++= scalaTest.value :: sparkBinaries.value)

lazy val joinwiz_testkit = project
  .dependsOn(joinwiz_core % "compile->compile;test->test")
  .settings(crossScalaVersions := supportedScalaVersions)
  .settings(commonSettings: _*)
  .settings(libraryDependencies ++= scalaTest.value :: sparkBinaries.value)

import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._
ThisBuild / organization := "io.github.salamahin"
ThisBuild / publishMavenStyle := true
ThisBuild / publishTo := sonatypePublishToBundle.value
ThisBuild / licenses := Seq("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"))
ThisBuild / homepage := Some(url("https://github.com/Salamahin/joinwiz"))
ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/Salamahin/joinwiz"),
    "scm:git@github.com:Salamahin/joinwiz.git"
  )
)
ThisBuild / developers := List(
  Developer(
    id = "Salamahin",
    name = "Danila Goloshchapov",
    email = "danilasergeevich@gmail.com",
    url = url("https://github.com/Salamahin")
  )
)

releaseIgnoreUntrackedFiles := true
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  releaseStepCommandAndRemaining("publishSigned"),
  releaseStepCommand("sonatypeBundleRelease"),
  setNextVersion,
  commitNextVersion,
  pushChanges
)
