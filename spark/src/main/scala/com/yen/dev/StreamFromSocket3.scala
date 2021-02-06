package com.yen.dev

// plz run socket at localhost:9999 before trigger this program
// nc -lk 9999

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object StreamFromSocket3 extends App {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("StreamFromSocket3")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions", 3)
    .getOrCreate()

  import spark.implicits._

  val linesDF = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", "9999")
    .load()

  val wordsDF = linesDF.select(expr("explode(split(value,' ')) as word"))

  println("***")
  wordsDF.printSchema()

  // let's replace the text in df here!
  // https://stackoverflow.com/questions/40080609/how-to-use-regexp-replace-in-spark
  // Note : have to "escape the string"
  // https://stackoverflow.com/questions/13948751/string-parse-error/13949358
  // e.g. indead of sysCallName.split("("), do sysCallName.split("\\(");
  // TODO : fix below workaround (e.g. can we do multiple "regexp_replace" in one df ?)
  val filterDF1 = wordsDF
    .withColumn("word", regexp_replace(wordsDF("word"), "\\(", ""))

  val filterDF2 = filterDF1
    .withColumn("word", regexp_replace(filterDF1("word"), "\\(", ""))

  val filterDF3 = filterDF2
    .withColumn("word", regexp_replace(filterDF2("word"), "\\?", ""))

  val filterDF4 = filterDF3
    .withColumn("word", regexp_replace(filterDF3("word"), "\\*", ""))

  println("***")
  filterDF4.printSchema()

  val countsDF = filterDF4.groupBy("word").count()

  val wordCountQuery = countsDF.writeStream
    .format("console")
    //.option("numRows", 2)
    .outputMode("complete")
    .option("checkpointLocation", "chk-point-dir") // set up the "checkpoint"
    .start()

  logger.info("Listening to localhost:9999")
  wordCountQuery.awaitTermination()
}
