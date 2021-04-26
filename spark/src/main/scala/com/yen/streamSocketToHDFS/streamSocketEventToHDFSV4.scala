package com.yen.streamSocketToHDFS


/**
 *  streamSocketEventToHDFSV4
 *
 *  1) spark streaming from socket to HDFS
 *  2) data type : words (dataframe)
 *  3) ref : https://github.com/yennanliu/Spark-Streaming-In-Scala/tree/master/01-StreamingWC
 *  4) ref : https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#handling-late-data-and-watermarking
 */

// plz open the other terminal an run below command as socket first
// nc -lk 9999

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

object streamSocketEventToHDFSV4 extends App {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("streamSocketEventToHDFS")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions", 3)
    .getOrCreate()

  import org.apache.spark.sql.functions._
  import spark.implicits._

  val lines = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", "9999")
    .load()

  // spark select as timestamp : https://sparkbyexamples.com/spark/spark-current-date-and-timestamp/

  val words = lines.select(
    expr("explode(split(value,' ')) as word"),
    // transform string to timestamp :  cast(TimestampType)
    // ref : https://docs.databricks.com/_static/notebooks/timestamp-conversion.html
    date_format(col("current_timestamp"),"yyyy-MM-dd HH:mm:ss.SSS").cast(TimestampType).as("time")
  )

  println("----------- words schema : -----------")
  words.printSchema()

  val windowedCounts = words
    .withWatermark("time", "10 minutes")
    .groupBy(
      window($"time", "10 minutes", "5 minutes"),
      $"word")
    .count()

  // save to HDFS
  // output format : https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-sinks
  // query the data :
  // hdfs dfs -ls spark/streamSocketEventToHDFS
  val wordCountQuery2 = windowedCounts.writeStream
    .format("json")
    .option("checkpointLocation", "chk-point-dir2") // set up the "checkpoint"
    .option("path", "streamSocketEventToHDFSV4")
    .start()

  logger.info("Listening to localhost:9999")
  wordCountQuery2.awaitTermination()
}
