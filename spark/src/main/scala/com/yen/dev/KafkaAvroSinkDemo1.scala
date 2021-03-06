package com.yen.dev

// spark streaming process stream from kafka and save back (sink) to kafka with apache avro format
// https://github.com/yennanliu/KafkaSparkPoc/blob/main/exampleCode/Spark-Streaming-In-Scala-master/06-KafkaAvroSinkDemo/src/main/scala/guru/learningjournal/spark/examples/KafkaAvroSinkDemo.scala

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.to_avro //.functions.to_avro
import org.apache.spark.sql.functions.{col, expr, from_json, struct}
import org.apache.spark.sql.types._

object KafkaAvroSinkDemo1 extends App{
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

  val kafkaTopic = "invoices_avro"
  val kafkaSourceDF = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", kafkaTopic)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()

  val valueDF = kafkaSourceDF.select(from_json(col("value").cast("string"), schema).alias("value"))

  val explodeDF = valueDF.selectExpr("value.InvoiceNumber", "value.CreatedTime", "value.StoreID",
    "value.PosID", "value.CustomerType", "value.CustomerCardNo", "value.DeliveryType", "value.DeliveryAddress.City",
    "value.DeliveryAddress.State", "value.DeliveryAddress.PinCode", "explode(value.InvoiceLineItems) as LineItem")

  val flattenedDF = explodeDF
    .withColumn("ItemCode", expr("LineItem.ItemCode"))
    .withColumn("ItemDescription", expr("LineItem.ItemDescription"))
    .withColumn("ItemPrice", expr("LineItem.ItemPrice"))
    .withColumn("ItemQty", expr("LineItem.ItemQty"))
    .withColumn("TotalValue", expr("LineItem.TotalValue"))
    .drop("LineItem")

  val kafkaTargetDF = flattenedDF.select(expr("InvoiceNumber as key"),
    to_avro(struct("*")).alias("value"))

  val outPutKafkaTopic = "invoice_avro_output"
  val invoiceWriterQuery = kafkaTargetDF
    .writeStream
    .queryName("Flattened Invoice Writer")
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", outPutKafkaTopic)
    .outputMode("append")
    .option("checkpointLocation", "chk-point-dir")
    .option("failOnDataLoss", "false")
    .start()

  logger.info("Start Writer Query")
  invoiceWriterQuery.awaitTermination()
}
