package com.yen.dev

// https://github.com/yennanliu/KafkaSparkPoc/blob/main/exampleCode/Spark-Streaming-In-Scala-master/03-KafkaStreamDemo/src/main/scala/guru/learningjournal/spark/examples/KafkaStreamDemo.scala

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, from_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object StreamFromKafkaWithSchema1 extends App{
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  println("StreamFromKafkaWithSchema1 run ...")

  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("StreamFromKafkaWithSchema1")
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
  
  // get df from kafka
  val kafkaDF = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "invoices")
    .option("startingOffsets", "earliest")
    .load()

  //kafkaDF.printSchema()

  // select stream key, value as new df
  val valueDF = kafkaDF.select(from_json(col("value").cast("string"), schema).alias("value"))

  // only filter out the needed columns
  val explodeDF = valueDF.selectExpr("value.InvoiceNumber", "value.CreatedTime", "value.StoreID",
    "value.PosID", "value.CustomerType", "value.PaymentMethod", "value.DeliveryType", "value.DeliveryAddress.City",
    "value.DeliveryAddress.State", "value.DeliveryAddress.PinCode", "explode(value.InvoiceLineItems) as LineItem") // notice this "explode" method

  // flatten the df columns via the "expr" method
  val flattenedDF = explodeDF
    .withColumn("ItemCode", expr("LineItem.ItemCode"))
    .withColumn("ItemDescription", expr("LineItem.ItemDescription"))
    .withColumn("ItemPrice", expr("LineItem.ItemPrice"))
    .withColumn("ItemQty", expr("LineItem.ItemQty"))
    .withColumn("TotalValue", expr("LineItem.TotalValue"))
    .drop("LineItem") // drop the no needed column

  // write stream df to kafka
  val invoiceWriterQuery = flattenedDF.writeStream
    .format("json")
    .queryName("Flattened Invoice Writer")
    .outputMode("append")
    .option("path", "output")
    .option("checkpointLocation", "chk-point-dir")
    .trigger(Trigger.ProcessingTime("1 minute"))
    .start()

  logger.info("Listening to Kafka")
  invoiceWriterQuery.awaitTermination()
}
