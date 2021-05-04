package com.yen.streamKafkaToHDFS

// event source :
// https://github.com/yennanliu/KafkaSparkPoc/blob/main/kafka/src/main/scala/com/yen/Producer/producerV1.scala

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object streamKafkaToHDFSV1 extends App {

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
  val topic = "raw_data"

  // subscribe to topic
  val streamDF = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootStrapServers)
    .option("subscribe", topic)
    .load()

  val tmpStreamDF = streamDF.selectExpr("CAST(value AS STRING)")

  val ToStreamDF = tmpStreamDF
    .select("value")

  ToStreamDF.createOrReplaceTempView("to_stream")

  // print out in console
  //  val query = ToStreamDF.writeStream
  //    .format("console")
  //    .option("checkpointLocation", "chk-point-dir2")
  //    .start()

  // save into HDFS
  val query = ToStreamDF.writeStream
      .format("json")
      .option("checkpointLocation", "chk-point-dir2")
      .option("path", "streamKafkaToHDFSV1")
      .start()

  logger.info("Listen from Kafka : 127.0.0.1:9092")

  query.awaitTermination()
}
