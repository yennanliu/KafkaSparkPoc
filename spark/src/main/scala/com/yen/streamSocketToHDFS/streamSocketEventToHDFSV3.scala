package com.yen.streamSocketToHDFS

/**
 *  streamSocketEventToHDFSV3
 *
 *  1) spark streaming json data from socket and make watermark, then group by with window function then save (sink) to HDFS
 *  3) ref :  https://github.com/yennanliu/KafkaSparkPoc/blob/main/spark/src/main/scala/com/yen/dev/TumblingWindowDemo1.scala
 */

// plz open the other terminal an run below command as socket first
// nc -lk 9999
// example input :
// {"CreatedTime": "2019-02-05 10:05:00", "Type": "BUY", "Amount": 500, "BrokerCode": "ABX"}
// {"CreatedTime": "2019-02-05 10:12:00", "Type": "BUY", "Amount": 300, "BrokerCode": "ABX"}
// {"CreatedTime": "2019-02-05 10:20:00", "Type": "BUY", "Amount": 800, "BrokerCode": "ABX"}

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object streamSocketEventToHDFSV3 extends App {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("streamSocketEventToHDFSV3")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions", 2)
    .getOrCreate()

  import org.apache.spark.sql.functions._
  import spark.implicits._

  // define schema
  // TODO : see if we can do it via case class
  val stockSchema = StructType(List(
    StructField("CreatedTime", StringType),
    StructField("Type", StringType),
    StructField("Amount", IntegerType),
    StructField("BrokerCode", StringType)
  ))

  val lines = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", "9999")
    .load()

  // process read stream df with defined schema
  val valueDF = lines.select(
    from_json(col("value").cast("string"), stockSchema)
    .alias("value")
  )

  val tradeDF = valueDF.select("value.*")
    .withColumn("CreatedTime", to_timestamp(col("CreatedTime"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("Buy", expr("case when Type == 'BUY' then Amount else 0 end"))  // will create a new column : "BUY" when Type == 'BUY'
    .withColumn("Sell", expr("case when Type == 'SELL' then Amount else 0 end")) // will create a new column : "SELL" when Type == 'SELL'

  val windowAggDF = tradeDF
    .groupBy( // col("BrokerCode"),
      window(col("CreatedTime"), "15 minute"))
    .agg(sum("Buy").alias("TotalBuy"),
      sum("Sell").alias("TotalSell"))

  val outputDF = windowAggDF
    .select("window.start", "window.end", "TotalBuy", "TotalSell")

  println("----------- valueDF schema : -----------")
  valueDF.printSchema()
  println("----------- tradeDF schema : -----------")
  tradeDF.printSchema()
  println("----------- windowAggDF schema : -----------")
  windowAggDF.printSchema()
  println("----------- outputDF schema : -----------")
  outputDF.printSchema()

  val outputDF2 = tradeDF
    .select("CreatedTime", "Buy", "Sell")
    .withWatermark("CreatedTime", "2 minutes")
    .groupBy(
      window($"CreatedTime", "10 minutes", "5 minutes"),
      $"Buy")
    .count()

  //  val windowQuery = outputDF.writeStream
  //    .format("console")
  //    .outputMode("update")
  //    .option("checkpointLocation", "chk-point-dir")
  //    .trigger(Trigger.ProcessingTime("1 minute"))
  //    .start()

  val windowQuery = outputDF2.writeStream
    .format("json")
    .option("checkpointLocation", "chk-point-dir")
    .trigger(Trigger.ProcessingTime("1 minute"))
    .option("path", "streamSocketEventToHDFSV3")
    .start()

  logger.info("Counting Invoices")
  windowQuery.awaitTermination()
}
