lazy val commonSettings = Seq (
    version := "0.1",
    organization := "github",
    scalaVersion := "2.11.7",
    scalacOptions := Seq("-unchecked", "-deprecation", "-feature", "-encoding", "utf8"),
    resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/releases"
)

lazy val testDependencies = Seq(
    "org.specs2" %% "specs2" % "3.+" % "test",
    "org.scalatest" %% "scalatest" % "2.2.6" % "test"
)

lazy val commonDependencies = Seq(
    "com.amazonaws" % "aws-java-sdk" % "1.10.+",
    "org.scalaz" %% "scalaz-core" % "7.2.0",
    "org.slf4j" % "jcl-over-slf4j" % "1.7.+",
    "ch.qos.logback" % "logback-classic" % "1.1.+",
    //"com.iheart" %% "ficus" % "1.2.+",
    "joda-time" % "joda-time" % "2.9.3"
)

lazy val dynamodbscala = project.in(file("."))
    .settings(commonSettings:_*)
    .settings(libraryDependencies ++= commonDependencies)
    .settings(libraryDependencies ++= testDependencies)

parallelExecution in Test := false
