package com.yen.streamToHDFS


// spark streaming from socket to HDFS
// https://github.com/yennanliu/Spark-Streaming-In-Scala/tree/master/01-StreamingWC

// plz open the other terminal an run below command as socket first
// nc -lk 9999

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object streamSocketEventToHDFS extends App {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("StreamFromSocket")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions", 3)
    .getOrCreate()

  val lines = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", "9999")
    .load()

  val words = lines.select(expr("explode(split(value,' ')) as word"))
  val counts = words.groupBy("word").count()

  val wordCountQuery = counts.writeStream
    .format("console")
    //.option("numRows", 2)
    .outputMode("complete")
    .option("checkpointLocation", "chk-point-dir") // set up the "checkpoint"
    .start()

  // save to HDFS
  // output format : https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-sinks
  val countsDF = counts.toDF()

  countsDF.writeStream
      .format("parquet")
      .option("path", "hdfs://KafkaSparkPoc/output/")
      .start()

  logger.info("Listening to localhost:9999")
  wordCountQuery.awaitTermination()
}
