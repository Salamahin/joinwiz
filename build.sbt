import sbt.url

name := "joinwiz"

lazy val sparkMajorVersion = sys.props.getOrElse("spark.version", "3")

lazy val supportedScalaVersions = sparkMajorVersion match {
  case "2" => List("2.11.12", "2.12.14")
  case "3" => List("2.13.8")
  case "4" => List("2.13.17")
  case v   => sys.error(s"Unsupported spark.version: $v. Use 2, 3, or 4.")
}

lazy val defaultScalaVersion = sparkMajorVersion match {
  case "2" => "2.12.14"
  case "3" => "2.13.8"
  case "4" => "2.13.17"
  case v   => sys.error(s"Unsupported spark.version: $v. Use 2, 3, or 4.")
}

lazy val sparkSuffix = s"-spark$sparkMajorVersion"

lazy val sparkV = Def.setting {
  sparkMajorVersion match {
    case "2" => scalaVersion.value match {
      case "2.11.12" => "2.3.2"
      case "2.12.14" => "2.4.5"
    }
    case "3" => "3.2.1"
    case "4" => "4.1.0"
  }
}

lazy val scalaTest    = Def.setting { "org.scalatest"    %% "scalatest"    % "3.2.19" % Test }
lazy val scalaReflect = Def.setting { "org.scala-lang"   % "scala-reflect" % scalaVersion.value }
lazy val sparkCore    = Def.setting { "org.apache.spark" %% "spark-core"   % sparkV.value }
lazy val sparkSql     = Def.setting { "org.apache.spark" %% "spark-sql"    % sparkV.value }

ThisBuild / scalaVersion       := defaultScalaVersion
ThisBuild / crossScalaVersions := supportedScalaVersions
ThisBuild / organization := "io.github.salamahin"
ThisBuild / homepage := Some(url("https://github.com/Salamahin/joinwiz"))
ThisBuild / developers := List(
  Developer(
    id    = "Salamahin",
    name  = "Danila Goloshchapov",
    email = "danilasergeevich@gmail.com",
    url   = url("https://github.com/Salamahin")
  )
)
ThisBuild / versionScheme := Some("early-semver")
ThisBuild / licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0"))
ThisBuild / scalacOptions ++= Seq(
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

lazy val commonSettings = Seq(
  libraryDependencies ++= scalaReflect.value :: sparkCore.value :: sparkSql.value :: Nil
)

lazy val root = (project in file("."))
  .aggregate(joinwiz_macro, joinwiz_core)
  .settings(commonSettings: _*)
  .settings(publish / skip := true)

lazy val joinwiz_macro = (project in file("joinwiz_macro"))
  .settings(name := s"joinwiz_macro$sparkSuffix")
  .settings(commonSettings: _*)

lazy val joinwiz_core = (project in file("joinwiz_core"))
  .settings(name := s"joinwiz_core$sparkSuffix")
  .dependsOn(joinwiz_macro)
  .settings(commonSettings: _*)
  .settings(libraryDependencies += scalaTest.value)
