package com.yen.dev

// https://github.com/yennanliu/KafkaSparkPoc/blob/main/exampleCode/Spark-Streaming-In-Scala-master/09-SlidingWindowDemo/src/main/scala/guru/learningjournal/spark/examples/SlidingWindowDemo.scala

// TODO : validate/check if this process is working

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object SlidingWindowDemo extends App {
  @transient lazy val logger:Logger = Logger.getLogger(getClass.getName)

  val spark = SparkSession.builder()
    .master("local[3]")
    .appName(this.getClass.getName)
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions", 1)
    .getOrCreate()

  val invoiceSchema = StructType(List(
    StructField("CreatedTime", StringType),
    StructField("Reading", DoubleType)
  ))

  // read stream df from kafka
  val kafkaSourceDF = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "sensor")
    .option("startingOffsets", "earliest")
    .load()

  // transform "key", and "value" from stream DF from kafka
  // and alias them
  val valueDF = kafkaSourceDF.select(col("key").cast("string").alias("SensorID"),
    from_json(col("value").cast("string"), invoiceSchema).alias("value"))

  val sensorDF = valueDF.select("SensorID", "value.*")
    .withColumn("CreatedTime", to_timestamp(col("CreatedTime"), "yyyy-MM-dd HH:mm:ss"))

  val aggDF = sensorDF
    .withWatermark("CreatedTime", "30 minute")
    .groupBy(
      col("SensorID"),
      window(col("CreatedTime"), "15 minute", "5 minute"))
    .agg(max("Reading").alias("MaxReading"))

  val outputDF = aggDF.select("SensorID", "window.start", "window.end", "MaxReading")

  val windowQuery = outputDF
    .writeStream
    .format("console")
    .outputMode("update")
    .option("checkpointLocation", "chk-point-dir")
    .start()

  logger.info("Counting Invoices")

  windowQuery.awaitTermination()
}
