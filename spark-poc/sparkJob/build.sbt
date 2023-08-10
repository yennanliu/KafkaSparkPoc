ThisBuild / version := "0.1.0-SNAPSHOT"

//ThisBuild / scalaVersion := "2.13.10"
ThisBuild / scalaVersion := "2.12.5"

resolvers += Resolver.sonatypeRepo("snapshots")

val sparkVersion = "3.3.1"

lazy val root = (project in file("."))
  .settings(
    name := "sparkJob"
  )

libraryDependencies ++= Seq(

  // spark batch
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,

  // spark kinesis
  "org.apache.spark" %% "spark-streaming-kinesis-asl" %  sparkVersion,

  // write to s3
  "org.apache.spark" %% "spark-hadoop-cloud" % sparkVersion,

  "com.typesafe" % "config" % "1.3.1",
  "com.github.scopt" %% "scopt" % "3.6.0"
)

conflictManager := ConflictManager.latestRevision

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}