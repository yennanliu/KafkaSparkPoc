package com.yen.streamSocketToHDFS

/**
 * streamSocketEventToHDFSV5
 *
 * 1) spark job digest event generated from com.yen.eventCreator.SimpleEvent_V1 and save to HDFS
 * 2) input data : json, output data : json in HDFS
 * 3) will read the socket event with schema and print output in console
 * 4) ref : https://github.com/yennanliu/KafkaSparkPoc/blob/main/spark/src/main/scala/com/yen/eventCreator/SimpleEvent_V1.scala
 */

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

object streamSocketEventToHDFSV5 extends App {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  val spark = SparkSession.builder()
    .master("local[3]")
    .appName(this.getClass.getName)
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions", 3)
    .getOrCreate()

  val eventSchema = StructType(List(
    StructField("id", IntegerType),
    StructField("event_date", LongType),
    StructField("msg", StringType)
  ))

  val lineDF = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", "9999")
    .load()

  val  valueDF = lineDF.select(
    from_json(col("value").cast("string"), eventSchema)
      .alias("value")
  )

  val eventDF = valueDF.select("value.*")
    //.withColumn("CreatedTime", to_timestamp(col("CreatedTime"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("id", expr("id"))
    .withColumn("event_date", expr("event_date"))
    .withColumn("msg", expr("msg"))

  eventDF.printSchema()

  val query = eventDF.writeStream
    .format("console")
    .option("checkpointLocation", "chk-point-dir")
    .start()

  query.awaitTermination()
}
