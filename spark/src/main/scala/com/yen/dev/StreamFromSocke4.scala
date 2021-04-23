package com.yen.dev

// spark streaming from socket data source
// https://github.com/yennanliu/Spark-Streaming-In-Scala/tree/master/01-StreamingWC

// plz open the other terminal an run below command as socket first
// nc -lk 9999

import com.yen.streamToHDFS.streamSocketEventToHDFS.lines
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// TODO : check how to run with "Serializable"
//object StreamFromSocket extends Serializable
object StreamFromSocket4 extends App{
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("StreamFromSocket")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions", 3)
    .getOrCreate()

  val linesDF = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", "9999")
    .load()

  //val wordsDF = linesDF.select(explode(split(col("value"), " ")).alias("word"))
  val wordsDF = linesDF.select(
    expr("explode(split(value,' ')) as word"),
    date_format(col("current_timestamp"),"yyyy-MM-dd HH:mm:ss.SSS").as("time")
  )

  val wordCountQuery = wordsDF.writeStream
    .format("console")
    .option("checkpointLocation", "chk-point-dir") // set up the "checkpoint"
    .start()

  logger.info("Listening to localhost:9999")
  wordCountQuery.awaitTermination()
}
