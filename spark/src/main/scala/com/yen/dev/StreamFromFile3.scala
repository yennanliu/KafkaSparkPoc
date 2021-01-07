package com.yen.dev

// spark streaming from file with defined schema and column selectExpr, expr..

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{BooleanType, StringType, StructField, StructType}

object StreamFromFile3 extends App{

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("StreamFromFile3")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.streaming.schemaInference", "true")
    .getOrCreate()

  val schema = StructType(
    List(
      StructField("kind", StringType),
      StructField("etag", StringType),
      StructField(
        "items",
        StructType(
          List(
            StructField("kind", StringType),
            StructField("etag", StringType),
            StructField("id", StringType),
            StructField(
              "snippet",
              StructType(
                List(
                  StructField("channelId", StringType),
                  StructField("title", StringType),
                  StructField("assignable", BooleanType)
                )
              )
            )
          )
        )
      )
    )
  )

  val rawDF = spark.readStream
    .format("json")
    .option("path", "../data/GB_category_id.json")
    .option("maxFilesPerTrigger", 1)
    .schema(schema)  // https://spark.apache.org/docs/2.1.1/structured-streaming-programming-guide.html
    .load()

  rawDF.printSchema()

  // let's select columns from the rawDF
  val explodeDF = rawDF.selectExpr("kind", "etag", "items.kind", "items.snippet.channelId" )

  explodeDF.printSchema()

  // show in console
  val query = explodeDF
    .writeStream
    .format("console")
    .outputMode(OutputMode.Append())
    .start()

  query.awaitTermination()
}

