package com.yen.dev

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{BooleanType, StringType, StructField, StructType}

object StreamFromFileSendToKafka1 extends App{

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("StreamFromFileSendToKafka1")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.streaming.schemaInference", "true")
    .getOrCreate()

  val schema = StructType(
      List(
        StructField("uid", StringType),
        StructField("name", StringType),
        StructField("msg", StringType)
      )
    )

  val rawDF = spark.readStream
    .format("json")
    .option("path", "../data/SampleData04/data/*.json")
    .option("maxFilesPerTrigger", 1)
    .schema(schema)  // https://spark.apache.org/docs/2.1.1/structured-streaming-programming-guide.html
    .load()

  //rawDF.printSchema()

  val parseDF = rawDF
    .selectExpr("uid", "name", "msg")

  parseDF.printSchema()

  // clean the data
  
  // WIP
}
