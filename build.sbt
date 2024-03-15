import sbt.url

name := "joinwiz"

releaseCrossBuild := true
inThisBuild(List(
  organization := "io.github.salamahin",
  homepage := Some(url("https://github.com/Salamahin/joinwiz")),
  licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
  developers := List(
    Developer(id = "Salamahin", name = "Danila Goloshchapov", email = "danilasergeevich@gmail.com", url = url("https://github.com/Salamahin"))
  )
))

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
  ),
  crossScalaVersions := List("2.13.8", "2.12.14", "2.11.12")
)

val sparkV       = Map("2.11" -> "2.3.2", "2.12" -> "2.4.5", "2.13" -> "3.2.1")
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
