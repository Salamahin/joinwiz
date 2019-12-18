name := "joinwiz"
organization in ThisBuild := "joinwiz"
scalaVersion in ThisBuild := "2.11.12"
homepage := Some(url("https://github.com/salamahin/joinwiz"))
publishMavenStyle := true

publishTo := Some(
  if (isSnapshot.value)
    Opts.resolver.sonatypeSnapshots
  else
    Opts.resolver.sonatypeStaging
)

lazy val global = project
  .in(file("."))
  .aggregate(
    joinwiz_core,
    joinwiz_macros,
    joinwiz_testkit
  )

lazy val commonSettings = Seq(
  scalacOptions ++= Seq(
    "-encoding",
    "utf8"
  )
)

lazy val joinwiz_macros = project
  .settings(
    commonSettings,
    libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value
  )

lazy val joinwiz_core = project
  .dependsOn(joinwiz_macros)
  .settings(
    commonSettings,
    libraryDependencies ++= Seq(dependencies.sparkCore, dependencies.sparkSql, dependencies.scalatest)
  )

lazy val joinwiz_testkit = project
  .dependsOn(joinwiz_core)
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
