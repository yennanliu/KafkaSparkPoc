package com.yen.dev

// spark streaming from file
// https://github.com/yennanliu/NYC_Taxi_Pipeline/blob/master/src/main/scala/EventLoad/FileStreamExample.scala

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object StreamFromFile extends App{

  val sparkSession = SparkSession.builder
    .master("local")
    .appName("StreamFromFile")
    .getOrCreate()

  val schema = StructType(
    Array(StructField("id", StringType),
      StructField("name", StringType),
      StructField("amount", StringType))
  )

  // stream from file
  val streamDF = sparkSession
    .readStream
    .option("header", "true")
    .schema(schema)
    .csv("../data/dev")

  // show in console
  val query = streamDF
    .writeStream
    .format("console")
    .outputMode(OutputMode.Append())
    .start()

  query.awaitTermination()
}
