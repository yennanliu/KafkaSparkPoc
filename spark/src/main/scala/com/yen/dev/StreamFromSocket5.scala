package com.yen.dev

/**
 * StreamFromSocket5
 *
 * 1) spark job digest event generated from com.yen.eventCreator.SimpleEvent_V1
 * 2) will read the socket event with schema and print output in console
 * 3) ref : https://github.com/yennanliu/KafkaSparkPoc/blob/main/spark/src/main/scala/com/yen/eventCreator/SimpleEvent_V1.scala
 *
 */

// plz open the other terminal an run below command as socket first
// nc -lk 9999

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object StreamFromSocket5 extends App {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("StreamFromSocket5")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions", 3)
    .getOrCreate()

  val lineDF = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", "9999")
    .load()

  val wordsDF = lineDF.select(expr("explode(split(value,' ')) as word"))

  lineDF.printSchema()

  val query = lineDF.writeStream
    .format("console")
    .option("checkpointLocation", "chk-point-dir")
    .start()

  query.awaitTermination()
}
