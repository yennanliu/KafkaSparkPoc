name := "spark-app"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.0"

resolvers += Resolver.sonatypeRepo("snapshots")

libraryDependencies ++= Seq(
  // config
  "com.typesafe" % "config" % "1.2.1", 
  "org.apache.spark" %% "spark-core" % "2.4.3",
  "org.scalactic" %% "scalactic" % "3.1.0",
  "org.apache.spark" %% "spark-sql" % "2.4.3",
  "com.databricks" %% "spark-csv" % "1.4.0",

  // spark stream 
   "org.apache.spark" %% "spark-streaming" % "2.4.3",
   "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.3",
  // "org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % "2.2.1",
  // "org.apache.spark" %% "spark-streaming-kinesis-asl" % "2.2.1",
  // "org.apache.bahir" %% "spark-streaming-twitter" % "2.3.1",
  // "com.danielasfregola" %% "twitter4s" % "6.1"

  // others 
  "org.apache.commons" % "commons-text" % "1.8",

  // test
  "org.scalatest" %% "scalatest" % "3.1.1" % "test"
)

conflictManager := ConflictManager.latestRevision