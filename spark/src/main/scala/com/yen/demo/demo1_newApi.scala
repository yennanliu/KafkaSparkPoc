package com.yen.demo

import org.apache.spark.sql
import org.apache.spark.sql.{Dataset, SparkSession}

object demo1_newApi extends App{

  val spark = SparkSession
    .builder
    .appName("demo1_newApi")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  // Create DataFrame representing the stream of input lines from connection to localhost:9999
  val lines = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()

  // Split the lines into words
  val words: Dataset[String] = lines.as[String].flatMap(_.split(" "))

  // mapping
  val pairs: Dataset[(String, Int)] = words.map(word => (word, 1))

  // Generate running word count
  val wordCounts:sql.DataFrame = words.groupBy("value").count()

  // Start running the query that prints the running counts to the console
  val query = wordCounts.writeStream
    .outputMode("complete")
    .queryName("traffic")
    .format("console")
    .start()

  query.awaitTermination()
}
