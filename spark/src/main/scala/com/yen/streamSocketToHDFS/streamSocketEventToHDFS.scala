package com.yen.streamSocketToHDFS


/**
 *  streamSocketEventToHDFS
 *
 *  1) spark streaming from socket to HDFS
 *  2) data type : words (dataframe)
 *  3) ref :   https://github.com/yennanliu/Spark-Streaming-In-Scala/tree/master/01-StreamingWC
 */

// plz open the other terminal an run below command as socket first
// nc -lk 9999

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object streamSocketEventToHDFS extends App {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("streamSocketEventToHDFS")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions", 3)
    .getOrCreate()

  val lines = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", "9999")
    .load()

  // spark select as timestamp : https://sparkbyexamples.com/spark/spark-current-date-and-timestamp/
  val words = lines.select(
    expr("explode(split(value,' ')) as word"),
    date_format(col("current_timestamp"),"yyyy-MM-dd HH:mm:ss.SSS").as("time")
  )
  
  // save to HDFS
  // output format : https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-sinks
  // query the data :
  // hdfs dfs -ls spark/streamSocketEventToHDFS
  val wordCountQuery2 = words.writeStream
      .format("json")
      .option("checkpointLocation", "chk-point-dir2") // set up the "checkpoint"
      .option("path", "streamSocketEventToHDFS")
      .start()

  logger.info("Listening to localhost:9999")
  wordCountQuery2.awaitTermination()
}
