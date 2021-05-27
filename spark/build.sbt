
version := "0.0.1"
organization:= "com.yen"
name := "spark-app"
scalaVersion := "2.12.12"

lazy val versions = new {
  val spark = "3.0.1"
  val log4j = "1.7.25"
  val typesafeConfig = "1.4.0"
  val json4s = "3.6.0"
  val scalaTest = "3.0.3"
}
resolvers += Resolver.sonatypeRepo("snapshots")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % versions.spark % "provided",
  "org.apache.spark" %% "spark-sql" % "3.0.1" % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.0.1" % Test,
  "org.apache.spark" %% "spark-streaming" % "3.0.1" % "provided",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.0.1",
  "org.slf4j" % "slf4j-api" % versions.log4j,
  "com.typesafe" % "config" % versions.typesafeConfig,
  "org.json4s" %% "json4s-native" % versions.json4s,
  "org.scalatest" %% "scalatest" % versions.scalaTest % Test
)

conflictManager := ConflictManager.latestRevision

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}