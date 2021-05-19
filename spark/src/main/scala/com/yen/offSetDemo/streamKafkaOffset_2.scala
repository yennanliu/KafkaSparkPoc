package com.yen.offSetDemo

import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object streamKafkaOffset_2 extends App {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  val spark = SparkSession
    .builder
    .appName(this.getClass.getName)
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
    .getOrCreate()

  import spark.implicits._

  // kafka config
  val bootStrapServers = "127.0.0.1:9092"
  // digest event from a list of topics
  val topic = "raw_data_1,raw_data_2,raw_data_3"

  val streamDF = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootStrapServers)
    .option("subscribe",topic) // subscribe multiple topics : https://spark.apache.org/docs/2.2.0/structured-streaming-kafka-integration.html
    //.option("startingOffsets", """{"raw_data_1":{"0":23,"1":-2},"raw_data_2":{"0":-2}}""") // https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
    //.option("endingOffsets", """{"raw_data_1":{"0":50,"1":-1},"raw_data_2":{"0":-1}}""")
    //.option("startingOffsets", "earliest")
    //.option("endingOffsets", "latest")
    .load()

  streamDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .as[(String, String)]

  val ToStreamDF = streamDF
    .select("key", "value")

  /** V1 : print out in console */
  val query = ToStreamDF.writeStream
            .format("console")
            .option("checkpointLocation", "chk-point-dir2")
            .start()

  query.awaitTermination()
}
