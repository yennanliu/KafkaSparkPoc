package com.yen.dev

// spark streaming and processing from kafka json with defined schema
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

  // get df from kafka
  val kafkaTopic = "invoices4"
  val kafkaDF = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", kafkaTopic)
    .option("startingOffsets", "earliest")  // read from the beginning from every run via the spark check point
    .load()

  // kafkaDF.printSchema()
  // you should see the kafkaDF schema like below via above print schema command:
  //  root
  //  |-- key: binary (nullable = true)
  //  |-- value: binary (nullable = true)
  //  |-- topic: string (nullable = true)
  //  |-- partition: integer (nullable = true)
  //  |-- offset: long (nullable = true)
  //  |-- timestamp: timestamp (nullable = true)
  //  |-- timestampType: integer (nullable = true)

  // select stream key, value as new df
  val valueDF = kafkaDF.select(from_json(col("value").cast("string"), schema).alias("value"))
  //valueDF.printSchema()
  // you should see the valueDF schema like below via above print schema command:
  //  root
  //  |-- value: struct (nullable = true)
  //  |    |-- InvoiceNumber: string (nullable = true)
  //  |    |-- CreatedTime: long (nullable = true)
  //  |    |-- StoreID: string (nullable = true)
  //  |    |-- PosID: string (nullable = true)
  //  |    |-- CashierID: string (nullable = true)
  //  |    |-- CustomerType: string (nullable = true)
  //  |    |-- CustomerCardNo: string (nullable = true)
  //  |    |-- TotalAmount: double (nullable = true)
  //  |    |-- NumberOfItems: integer (nullable = true)
  //  |    |-- PaymentMethod: string (nullable = true)
  //  |    |-- CGST: double (nullable = true)
  //  |    |-- SGST: double (nullable = true)
  //  |    |-- CESS: double (nullable = true)
  //  |    |-- DeliveryType: string (nullable = true)
  //  |    |-- DeliveryAddress: struct (nullable = true)
  //  |    |    |-- AddressLine: string (nullable = true)
  //  |    |    |-- City: string (nullable = true)
  //  |    |    |-- State: string (nullable = true)
  //  |    |    |-- PinCode: string (nullable = true)
  //  |    |    |-- ContactNumber: string (nullable = true)
  //  |    |-- InvoiceLineItems: array (nullable = true)
  //  |    |    |-- element: struct (containsNull = true)
  //  |    |    |    |-- ItemCode: string (nullable = true)
  //  |    |    |    |-- ItemDescription: string (nullable = true)
  //  |    |    |    |-- ItemPrice: double (nullable = true)
  //  |    |    |    |-- ItemQty: integer (nullable = true)
  //  |    |    |    |-- TotalValue: double (nullable = true)


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
