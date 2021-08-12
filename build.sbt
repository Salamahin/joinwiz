import sbt.url
import sbtrelease.ReleaseStateTransformations._

name := "joinwiz"
releaseCrossBuild := true
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

ThisBuild / organization := "io.github.salamahin"
ThisBuild / crossScalaVersions := Seq("2.12.14", "2.11.12")
ThisBuild / publishMavenStyle := true
ThisBuild / publishTo := sonatypePublishToBundle.value
ThisBuild / licenses := Seq("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"))
ThisBuild / homepage := Some(url("https://github.com/Salamahin/joinwiz"))
ThisBuild / scmInfo := Some(ScmInfo(url("https://github.com/Salamahin/joinwiz"), "scm:git@github.com:Salamahin/joinwiz.git"))
ThisBuild / developers := List(
  Developer(id = "Salamahin", name = "Danila Goloshchapov", email = "danilasergeevich@gmail.com", url = url("https://github.com/Salamahin"))
)

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
    "-language:existentials",
    "-Ydelambdafy:inline"
  )
)
val sparkV       = Map("2.11" -> "2.3.2", "2.12" -> "2.4.5")
def scalaTest    = Def.setting { "org.scalatest" %% "scalatest" % "3.1.0" % Test }
def scalaReflect = Def.setting { "org.scala-lang" % "scala-reflect" % scalaVersion.value }
def sparkCore    = Def.setting { "org.apache.spark" %% "spark-core" % sparkV(scalaBinaryVersion.value) }
def sparkSql     = Def.setting { "org.apache.spark" %% "spark-sql" % sparkV(scalaBinaryVersion.value) }

lazy val joinwiz_macro = project
  .settings(commonSettings: _*)
  .settings(libraryDependencies ++= scalaReflect.value :: sparkCore.value :: sparkSql.value :: Nil)

lazy val joinwiz_core = project
  .dependsOn(joinwiz_macro)
  .settings(commonSettings: _*)
  .settings(libraryDependencies ++= scalaTest.value :: sparkCore.value :: sparkSql.value :: Nil)
