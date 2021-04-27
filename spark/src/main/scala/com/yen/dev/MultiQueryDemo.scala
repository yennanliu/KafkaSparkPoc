package com.yen.dev

// spark streaming process stream from kafka and with multiple query then save back (sink) to kafka
// https://github.com/yennanliu/KafkaSparkPoc/blob/main/exampleCode/Spark-Streaming-In-Scala-master/05-MultiQueryDemo/src/main/scala/guru/learningjournal/spark/examples/MultiQueryDemo.scala

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, from_json}
import org.apache.spark.sql.types._

object MultiQueryDemo extends App{

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  val spark = SparkSession.builder()
    .master("local[3]")
    .appName(this.getClass.getName)
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .getOrCreate()

  // *** define schema for stream df
  val schema = StructType(
    List(
      StructField("InvoiceNumber", StringType),
      StructField("CreatedTime", LongType),
      StructField("StoreID", StringType),
      StructField("PosID", StringType),
      StructField("CashierID", StringType),
      StructField("CustomerType", StringType),
      StructField("CustomerCardNo", StringType),
      StructField("TotalAmount", DoubleType),
      StructField("NumberOfItems", IntegerType),
      StructField("PaymentMethod", StringType),
      StructField("CGST", DoubleType),
      StructField("SGST", DoubleType),
      StructField("CESS", DoubleType),
      StructField("DeliveryType", StringType),
      StructField(
        "DeliveryAddress",
        StructType(
          List(
            StructField("AddressLine", StringType),
            StructField("City", StringType),
            StructField("State", StringType),
            StructField("PinCode", StringType),
            StructField("ContactNumber", StringType)
          )
        )
      ),
      StructField(
        "InvoiceLineItems",
        ArrayType(
          StructType(
            List(
              StructField("ItemCode", StringType),
              StructField("ItemDescription", StringType),
              StructField("ItemPrice", DoubleType),
              StructField("ItemQty", IntegerType),
              StructField("TotalValue", DoubleType)
            )
          )
        )
      )
    )
  )

  val kafkaTopic = "invoices6"
  val kafkaSourceDF = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", kafkaTopic)
    .option("startingOffsets", "earliest")
    .load()

  val valueDF = kafkaSourceDF.select(from_json(col("value").cast("string"), schema).alias("value"))

  val notificationDF = valueDF.select("value.InvoiceNumber", "value.CustomerCardNo", "value.TotalAmount")
    .withColumn("EarnedLoyaltyPoints", expr("TotalAmount * 0.2"))

  // select "InvoiceNumber" as key, and via to_json as value
  val kafkaTargetDF = notificationDF.selectExpr("InvoiceNumber as key",
    """to_json(named_struct('CustomerCardNo', CustomerCardNo,
      |'TotalAmount', TotalAmount,
      |'EarnedLoyaltyPoints', TotalAmount * 0.2
      |)) as value""".stripMargin)

  val outPutKafkaTopic = "notifications"
  val notificationWriterQuery = kafkaTargetDF
    .writeStream
    .queryName("Notification Writer")
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", outPutKafkaTopic)
    .outputMode("append")
    .option("checkpointLocation", "chk-point-dir/notify")
    .start()

  val explodeDF = valueDF.selectExpr("value.InvoiceNumber", "value.CreatedTime", "value.StoreID",
    "value.PosID", "value.CustomerType", "value.PaymentMethod", "value.DeliveryType", "value.DeliveryAddress.City",
    "value.DeliveryAddress.State", "value.DeliveryAddress.PinCode", "explode(value.InvoiceLineItems) as LineItem")

  val flattenedDF = explodeDF
    .withColumn("ItemCode", expr("LineItem.ItemCode"))
    .withColumn("ItemDescription", expr("LineItem.ItemDescription"))
    .withColumn("ItemPrice", expr("LineItem.ItemPrice"))
    .withColumn("ItemQty", expr("LineItem.ItemQty"))
    .withColumn("TotalValue", expr("LineItem.TotalValue"))
    .drop("LineItem")

  // save flattenedDF to /output file
  val invoiceWriterQuery = flattenedDF.writeStream
    .format("json")
    .queryName("Flattened Invoice Writer")
    .outputMode("append")
    .option("path", "output")
    .option("checkpointLocation", "chk-point-dir/flatten")
    .start()

  logger.info("Waiting for Queries")
  spark.streams.awaitAnyTermination()
}
