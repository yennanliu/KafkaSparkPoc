package com.yen.dev

// Spark stream from Kafka with Schema and process with Tumbling Window for total `buy and sell` values
// https://github.com/yennanliu/KafkaSparkPoc/blob/main/exampleCode/Spark-Streaming-In-Scala-master/08-TumblingWindowDemo/src/main/scala/guru/learningjournal/spark/examples/TumblingWindowDemo.scala

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object TumblingWindowDemo1 extends App {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  val spark = SparkSession.builder()
    .master("local[3]")
    .appName(this.getClass.getName)
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions", 2)
    .getOrCreate()

  // define schema
  val stockSchema = StructType(List(
    StructField("CreatedTime", StringType),
    StructField("Type", StringType),
    StructField("Amount", IntegerType),
    StructField("BrokerCode", StringType)
  ))

  val kafkaSourceTopic = "trades"
  val kafkaSourceDF = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", kafkaSourceTopic)
    .option("startingOffsets", "earliest")
    .load()

  val valueDF = kafkaSourceDF.select(from_json(col("value").cast("string"), stockSchema).alias("value"))

  val tradeDF = valueDF.select("value.*")
    .withColumn("CreatedTime", to_timestamp(col("CreatedTime"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("Buy", expr("case when Type == 'BUY' then Amount else 0 end"))  // will create a new column : "BUY" when Type == 'BUY'
    .withColumn("Sell", expr("case when Type == 'SELL' then Amount else 0 end")) // will create a new column : "SELL" when Type == 'SELL'

  //tradeDF.printSchema()

  //  root
  //  |-- CreatedTime: timestamp (nullable = true)
  //  |-- Type: string (nullable = true)
  //  |-- Amount: integer (nullable = true)
  //  |-- BrokerCode: string (nullable = true)
  //  |-- Buy: integer (nullable = true)
  //  |-- Sell: integer (nullable = true)

  // groupby window and agg on Buy, Sell by window as TotalBuy, TotalSell
  val windowAggDF = tradeDF
    .groupBy( // col("BrokerCode"),
      window(col("CreatedTime"), "15 minute"))
    .agg(sum("Buy").alias("TotalBuy"),
      sum("Sell").alias("TotalSell"))

  windowAggDF.printSchema()

  //  root
  //  |-- window: struct (nullable = false)
  //  |    |-- start: timestamp (nullable = true)
  //  |    |-- end: timestamp (nullable = true)
  //  |-- TotalBuy: long (nullable = true)
  //  |-- TotalSell: long (nullable = true)

  val outputDF = windowAggDF.select("window.start", "window.end", "TotalBuy", "TotalSell")

  /*
   val runningTotalWindow = Window.orderBy("end")
     .rowsBetween(Window.unboundedPreceding, Window.currentRow)
   val finalOutputDF = outputDF
     .withColumn("RTotalBuy", sum("TotalBuy").over(runningTotalWindow))
     .withColumn("RTotalSell", sum("TotalSell").over(runningTotalWindow))
     .withColumn("NetValue", expr("RTotalBuy - RTotalSell"))
   finalOutputDF.show(false)
   */

  val windowQuery = outputDF.writeStream
    .format("console")
    .outputMode("update")
    .option("checkpointLocation", "chk-point-dir")
    .trigger(Trigger.ProcessingTime("1 minute"))
    .start()

  logger.info("Counting Invoices")
  windowQuery.awaitTermination()
}
