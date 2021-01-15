package com.yen.dev

// spark streaming from file with column selectExpr, expr..
// https://github.com/yennanliu/Spark-Streaming-In-Scala/tree/master/02-FileStreamDemo

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object StreamFromFile2 extends App{
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("StreamFromFile2")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.streaming.schemaInference", "true")
    .getOrCreate()

  val rawDF = spark.readStream
    .format("json")
    .option("path", "../data/SampleData01/*.json")
    .option("maxFilesPerTrigger", 1)
    .load()

  /*  Notice here :
   *  1) select columns via selectExpr
   *  2) flatten columns via explode
   */
  val explodeDF = rawDF.selectExpr("InvoiceNumber", "CreatedTime", "StoreID", "PosID",
    "CustomerType", "PaymentMethod", "DeliveryType", "DeliveryAddress.City", "DeliveryAddress.State",
    "DeliveryAddress.PinCode", "explode(InvoiceLineItems) as LineItem")

  /*  Notice here :
   *  1) select sub columns via expr
   *  2) drop columns via drop
   */
  val flattenedDF = explodeDF
    .withColumn("ItemCode", expr("LineItem.ItemCode"))
    .withColumn("ItemDescription", expr("LineItem.ItemDescription"))
    .withColumn("ItemPrice", expr("LineItem.ItemPrice"))
    .withColumn("ItemQty", expr("LineItem.ItemQty"))
    .withColumn("TotalValue", expr("LineItem.TotalValue"))
    .drop("LineItem")

  // write output intp json format
  val output = "output"
  val invoiceWriterQuery = flattenedDF.writeStream
    .format("json")
    .queryName("Flattened Invoice Writer")
    .outputMode("append")
    .option("path", output)
    .option("checkpointLocation", "chk-point-dir")
    // trigger spark stream processing every 1 minute
    .trigger(Trigger.ProcessingTime("1 minute"))
    .start()

    logger.info("Flattened Invoice Writer started")
    invoiceWriterQuery.awaitTermination()
}
