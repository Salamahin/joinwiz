logLevel := Level.Debug

addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "3.8.1")
addSbtPlugin("com.jsuereth" % "sbt-pgp" % "2.0.0")
addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.12")

addCompilerPlugin("io.tryp" % "splain" % "0.5.0" cross CrossVersion.patch)