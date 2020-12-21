package com.yen.dev

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType,LongType,FloatType,DoubleType, TimestampType}
import org.apache.spark.SparkContext

// https://github.com/yennanliu/NYC_Taxi_Pipeline/blob/master/src/main/scala/KafkaEventLoad/LoadKafkaEventExample.scala

object StreamFromKafka extends App{

  val sc = new SparkContext("local[*]", "StreamFromKafka")

  val spark = SparkSession
    .builder
    .appName("StreamFromKafka")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
    .getOrCreate()

  import spark.implicits._

  // kafka config
  val bootStrapServers = "localhost:9092"
  val topic = "raw_data"

  // subscribe to topic
  val streamDF = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootStrapServers)
    .option("subscribe", topic)
    .load()

  streamDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]

  streamDF.printSchema

  streamDF.createGlobalTempView("event_table")

  val query = "SELECT * FROM event_table"

  spark.sql(query)
    .writeStream
    .format("console")
    .start()
    .awaitTermination()
}
