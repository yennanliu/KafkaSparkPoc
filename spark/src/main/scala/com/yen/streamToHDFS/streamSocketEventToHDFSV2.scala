package com.yen.streamToHDFS


/**
 *  streamSocketEventToHDFSV2
 *
 *  1) spark streaming from socket and make new timestamp relative columns
 *  2) window.start, window.end, last_ts, first_ts...
 */

// plz open the other terminal an run below command as socket first
// nc -lk 9999

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.current_timestamp

object streamSocketEventToHDFSV2 extends App {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("streamSocketEventToHDFSV2")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions", 3)
    .getOrCreate()

  val lines = spark.readStream.
    format("socket").
    option("host", "localhost").
    option("port", 9999).
    option("includeTimestamp", true).
    load()

  import org.apache.spark.sql.functions._
  import spark.implicits._

  // “value $ is not a member of StringContext” - https://stackoverflow.com/questions/44209756/value-is-not-a-member-of-stringcontext-missing-scala-plugin/44210165
  // window func example : https://stackoverflow.com/questions/46687491/spark-structured-streaming-aggregate-over-last-20-seconds
  val windowedCounts = lines.
    groupBy(
      window($"timestamp", "20 seconds"), $"value")
      .agg(count($"value") as "count",
      last($"timestamp") as "last_ts",
      first($"timestamp") as "first_ts").
    select($"window.start", $"window.end",  $"last_ts", $"first_ts", $"value", $"count")

  val query = windowedCounts.writeStream.
    outputMode("complete").
    format("console").
    option("truncate", false).
    start()

  query.awaitTermination()
}
