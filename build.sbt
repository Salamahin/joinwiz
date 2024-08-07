import sbt.url

name := "joinwiz"

lazy val scala213               = "2.13.8"
lazy val scala212               = "2.12.14"
lazy val scala211               = "2.11.12"
lazy val supportedScalaVersions = scala211 :: scala212 :: scala213 :: Nil

lazy val sparkV =
  Def.setting(scalaVersion.value match {
    case `scala211` => "2.3.2"
    case `scala212` => "2.4.5"
    case `scala213` => "3.2.1"
  })

lazy val scalaTest    = Def.setting { "org.scalatest"    %% "scalatest"    % "3.1.0" % Test }
lazy val scalaReflect = Def.setting { "org.scala-lang"   % "scala-reflect" % scalaVersion.value }
lazy val sparkCore    = Def.setting { "org.apache.spark" %% "spark-core"   % sparkV.value }
lazy val sparkSql     = Def.setting { "org.apache.spark" %% "spark-sql"    % sparkV.value }

sonatypeCredentialHost := "s01.oss.sonatype.org"
sonatypeRepository := "https://s01.oss.sonatype.org/service/local"

inThisBuild(
  List(
    organization := "io.github.salamahin",
    homepage := Some(url("https://github.com/Salamahin/joinwiz")),
    licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    developers := List(
      Developer(
        id    = "Salamahin",
        name  = "Danila Goloshchapov",
        email = "danilasergeevich@gmail.com",
        url   = url("https://github.com/Salamahin")
      )
    ),
    crossScalaVersions := supportedScalaVersions
  )
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
  ),
  crossScalaVersions := supportedScalaVersions,
  libraryDependencies ++= scalaReflect.value :: sparkCore.value :: sparkSql.value :: Nil
)

lazy val joinwiz_macro = project
  .settings(commonSettings: _*)

lazy val joinwiz_core = project
  .dependsOn(joinwiz_macro)
  .settings(commonSettings: _*)
  .settings(libraryDependencies += scalaTest.value)
