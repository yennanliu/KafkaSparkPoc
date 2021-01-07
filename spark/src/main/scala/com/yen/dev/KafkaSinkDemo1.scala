package com.yen.dev

// spark streaming process stream from kafka and save back (sink) to kafka
// https://github.com/yennanliu/KafkaSparkPoc/blob/main/exampleCode/Spark-Streaming-In-Scala-master/04-KafkaSinkDemo/src/main/scala/guru/learningjournal/spark/examples/KafkaSinkDemo.scala

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, from_json}
import org.apache.spark.sql.types._

object KafkaSinkDemo1 extends App{

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("KafkaSinkDemo1")
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

  // get stream df from kafka
  val kafkaTopic = "invoices5"
  val kafkaSourceDF = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", kafkaTopic)
    .option("startingOffsets", "earliest")
    .load()

  val valueDF = kafkaSourceDF.select(from_json(col("value").cast("string"), schema).alias("value"))

  // select needed columns, and modify value from one of them (expr("TotalAmount * 0.2"))
  val notificationDF = valueDF.select("value.InvoiceNumber", "value.CustomerCardNo", "value.TotalAmount")
    .withColumn("EarnedLoyaltyPoints", expr("TotalAmount * 0.2"))

  // if select all columns
  //val kafkaTargetDF = notificationDF.selectExpr("InvoiceNumber as key", "to_json(struct(*)) as value")

  // if on select needed columns
  val kafkaTargetDF = notificationDF.selectExpr("InvoiceNumber as key",
    """to_json(named_struct('CustomerCardNo', CustomerCardNo,
      |'TotalAmount', TotalAmount,
      |'EarnedLoyaltyPoints', TotalAmount * 0.2
      |)) as value""".stripMargin)


  // if output result in console; rather than sending to another kafka topic
  /*
val notificationWriterQuery = kafkaTargetDF.writeStream
  .format("console")
  .outputMode("append")
  .option("truncate", "false")
  .option("checkpointLocation", "chk-point-dir")
  .start()
*/

  // send output to another kafka topic
  val outPutKafkaTopic = "notifications"
  val notificationWriterQuery = kafkaTargetDF
    .writeStream
    .queryName("Notification Writer")
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", outPutKafkaTopic)
    .outputMode("append")
    .option("checkpointLocation", "chk-point-dir")
    .start()

  logger.info("Listening and writing to Kafka")
  notificationWriterQuery.awaitTermination()
}
