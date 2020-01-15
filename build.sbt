name := "joinwiz"
organization in ThisBuild := "io.github.salamahin"
scalaVersion in ThisBuild := "2.11.12"

lazy val commonSettings = Seq(
  scalacOptions ++= Seq(
    "-encoding", "utf8",
    "-Xlog-implicits"
  )
)

lazy val joinwiz_macro = project
  .settings(commonSettings: _*)
  .settings(
    libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value
  )

lazy val joinwiz_core = project
  .dependsOn(joinwiz_macro)
  .settings(commonSettings: _*)
  .settings(
    libraryDependencies ++= Seq(dependencies.sparkCore, dependencies.sparkSql, dependencies.scalatest)
  )

lazy val joinwiz_testkit = project
  .dependsOn(joinwiz_core)
  .settings(commonSettings: _*)
  .settings(libraryDependencies ++= Seq(dependencies.sparkCore, dependencies.sparkSql, dependencies.scalatest))

lazy val dependencies = new {
  val sparkV = "2.3.0"

  val sparkCore = "org.apache.spark" %% "spark-core" % sparkV % Provided
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkV % Provided
  val scalatest = "org.scalatest" %% "scalatest" % "3.1.0" % Test
}

import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._

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
  Developer(id = "Salamahin", name = "Danila Goloshchapov", email = "danilasergeevich@gmail.com", url = url("https://github.com/Salamahin"))
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