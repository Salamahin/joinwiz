name := "joinwiz"
organization in ThisBuild := "joinWiz"
scalaVersion in ThisBuild := "2.11.12"

lazy val global = project
  .in(file("."))
  .aggregate(
    core,
    macros
  )

lazy val commonSettings = Seq(
  scalacOptions ++= Seq(
    "-unchecked",
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
    libraryDependencies ++= Seq(dependencies.sparkCore, dependencies.sparkSql)
  )

lazy val dependencies = new {
  val sparkV = "2.3.0"

  val sparkCore = "org.apache.spark" %% "spark-core" % sparkV % Provided
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkV % Provided
}

lazy val assemblySettings = Seq(
  assemblyJarName in assembly := name.value + ".jar",
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", _*) => MergeStrategy.discard
    case _ => MergeStrategy.first
  }
)
