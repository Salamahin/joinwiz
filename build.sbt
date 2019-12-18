name := "joinwiz"
organization in ThisBuild := "io.github.salamahin"
scalaVersion in ThisBuild := "2.11.12"

lazy val global = project
  .in(file("."))
  .aggregate(
    core,
    macros
  )

lazy val commonSettings = Seq(
  scalacOptions ++= Seq(
    "-encoding",
    "utf8"
  )
)

lazy val macros = project
  .settings(
    commonSettings,
    libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value
  )

lazy val core = project
  .dependsOn(macros)
  .settings(
    commonSettings,
    libraryDependencies ++= Seq(dependencies.sparkCore, dependencies.sparkSql, dependencies.scalatest)
  )

lazy val dependencies = new {
  val sparkV = "2.3.0"

  val sparkCore = "org.apache.spark" %% "spark-core" % sparkV % Provided
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkV % Provided
  val scalatest = "org.scalatest" %% "scalatest" % "3.1.0" % Test
}

lazy val assemblySettings = Seq(
  assemblyJarName in assembly := name.value + ".jar",
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", _*) => MergeStrategy.discard
    case _ => MergeStrategy.first
  }
)

useGpg := true
ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/salamahin/joinwiz"),
    "scm:git@github.com:salamahin/joinwiz.git"
  )
)
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / licenses := List("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"))
ThisBuild / homepage := Some(url("https://github.com/salamahin/joinwiz"))
ThisBuild / publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}
ThisBuild / publishMavenStyle := true
