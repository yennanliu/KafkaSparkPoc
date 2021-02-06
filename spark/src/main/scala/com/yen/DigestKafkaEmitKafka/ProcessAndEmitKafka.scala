package com.yen.DigestKafkaEmitKafka

/*
 * SPARK PROGRAM THAT DIGEST KAFKA STREAM AND SEND CLEANED STREAM TO KAFKA WITH NEW TOPIC
 */

// https://github.com/yennanliu/KafkaSparkPoc/blob/main/spark/src/main/scala/com/yen/dev/StreamFromKafka.scala

import com.yen.dev.StreamFromKafka.tmpStreamDF
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.SparkContext

object ProcessAndEmitKafka extends App {

  val sc = new SparkContext("local[*]", "StreamFromKafka")

  val spark = SparkSession
    .builder
    .appName("ProcessAndEmitKafka")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
    .getOrCreate()

  import spark.implicits._

  // kafka config
  val bootStrapServers = "127.0.0.1:9092"
  val topic = "event_raw"

  // subscribe to topic
  val streamDF = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootStrapServers)
    .option("subscribe", topic)
    .load()

  val tmpStreamDF = streamDF.selectExpr("CAST(value AS STRING)")

  // let's clean the stream df here!

  val filterDF = tmpStreamDF
    .withColumn("value", regexp_replace(tmpStreamDF("value"), "\\???", ""))

  val ToStreamDF = tmpStreamDF
    .select("value")

  ToStreamDF.createOrReplaceTempView("to_stream")

  val query = "SELECT * FROM to_stream"

  // send the cleaned event to kafka with another topic
  val ToKafkaTopic = "event_clean"
  val notificationWriterQuery = ToStreamDF
    .writeStream
    .queryName("Notification Writer")
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", ToKafkaTopic)
    .outputMode("append")
    .option("checkpointLocation", "chk-point-dir")
    .start()
}
