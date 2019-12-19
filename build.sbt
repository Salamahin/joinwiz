name := "joinwiz"
organization in ThisBuild := "io.github.salamahin"
version := "0.11-SNAPSHOT"

scalaVersion in ThisBuild := "2.11.12"

lazy val commonSettings = Seq(
  scalacOptions ++= Seq("-encoding", "utf8")
)

lazy val assemblySettings = Seq(
  assemblyJarName in assembly := name.value + ".jar",
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", _*) => MergeStrategy.discard
    case _ => MergeStrategy.first
  }
)

lazy val joinwiz = project
  .in(file("."))
  .aggregate(joinwiz_core, joinwiz_macro)

lazy val joinwiz_macro = project
  .settings(
    commonSettings,
    libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value
  )

lazy val joinwiz_core = project
  .dependsOn(joinwiz_macro)
  .settings(
    commonSettings ++ assemblySettings,
    libraryDependencies ++= Seq(dependencies.sparkCore, dependencies.sparkSql, dependencies.scalatest)
  )

lazy val dependencies = new {
  val sparkV = "2.3.0"

  val sparkCore = "org.apache.spark" %% "spark-core" % sparkV % Provided
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkV % Provided
  val scalatest = "org.scalatest" %% "scalatest" % "3.1.0" % Test
}

sonatypeProfileName := "io.github.salamahin"
ThisBuild / publishMavenStyle := true
ThisBuild / publishTo := sonatypePublishToBundle.value
ThisBuild / licenses := Seq("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"))

import xerial.sbt.Sonatype._

sonatypeProjectHosting := Some(GitHubHosting("salamahin", "joinwiz", "danilasergeevich@gmail.com"))