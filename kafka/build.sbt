name := "kafaka-app"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.0"

resolvers += Resolver.sonatypeRepo("snapshots")
resolvers += "confluent.io" at "http://packages.confluent.io/maven/"

lazy val versions = new {
  val jodaConvert = "2.1"
  val jodaTime = "2.9.3"
  val log4j = "1.2.17"
}

libraryDependencies ++= Seq(
  // config
  "com.typesafe" % "config" % "1.2.1",

  // kafka
  "org.apache.kafka" % "kafka-clients" % "2.1.1",
  "org.apache.kafka" %% "kafka" % "2.1.1",
  "org.apache.kafka" % "kafka-streams" % "2.1.1",
  "org.apache.kafka" %% "kafka-streams-scala" % "2.0.0",
  "io.confluent" % "kafka-streams-avro-serde" % "5.0.0",
  "com.sksamuel.avro4s" %% "avro4s-core" % "1.9.0",

  // zookeeper
  // "org.apache.curator" % "curator-recipes" % "4.2.0",
  // "org.apache.curator" % "curator-test" % "4.2.0" % Test,

  // depedency
  "org.slf4j" % "slf4j-api" % "1.7.25",
  "org.slf4j" % "slf4j-simple" % "1.7.25" % Test,

  // test
  "org.scalatest" %% "scalatest" % "3.1.1" % "test",
  "org.apache.kafka" % "kafka-streams-test-utils" % "2.1.0" % "test",

  // time transform
  "joda-time" % "joda-time" % versions.jodaTime,
  "org.joda" % "joda-convert" % versions.jodaConvert,

  // log
  "log4j" % "log4j" % versions.log4j,
  "log4j" % "apache-log4j-extras" % versions.log4j,

  // json
  "org.json4s" %% "json4s-native" % "3.2.11",
  "org.json4s" %% "json4s-jackson" % "3.2.11"
)

conflictManager := ConflictManager.latestRevision