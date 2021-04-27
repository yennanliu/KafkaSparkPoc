package com.yen.dev

// plz run socket at localhost:9999 before trigger this program
// nc -lk 9999

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object StreamFromSocket2 extends App {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  val spark = SparkSession.builder()
    .master("local[3]")
    .appName(this.getClass.getName)
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

  // let's filter the df here!
  // https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
  // https://stackoverflow.com/questions/35759099/filter-spark-dataframe-on-string-contains
  val filterDF = wordsDF
      .filter($"word".contains("!@?#"))
      //.filter("word like '#'")
      //.filter("word > 10")

  val countsDF = filterDF.groupBy("word").count()

  val wordCountQuery = countsDF.writeStream
    .format("console")
    //.option("numRows", 2)
    .outputMode("complete")
    .option("checkpointLocation", "chk-point-dir") // set up the "checkpoint"
    .start()

  logger.info("Listening to localhost:9999")
  wordCountQuery.awaitTermination()

}
