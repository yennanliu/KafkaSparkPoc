package com.yen.dev

// https://github.com/yennanliu/KafkaSparkPoc/blob/main/exampleCode/Spark-Streaming-In-Scala-master/07-KafkaAvroSourceDemo/src/main/scala/guru/learningjournal/spark/examples/KafkaAvroSourceDemo.scala

import java.nio.file.{Files, Paths}

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.avro.functions.from_avro
import org.apache.spark.sql.avro.from_avro
import org.apache.spark.sql.functions.{col, expr, round, struct, sum, to_json}

object KafkaAvroSourceDemo1 extends App{
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  val spark = SparkSession.builder()
    .master("local[3]")
    .appName(this.getClass.getName)
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .getOrCreate()

  // *** WILL READ KAFKA STREAM FROM "KafkaAvroSinkDemo1"
  val kafkaSourceTopic = "invoice_avro_output"
  val kafkaSourceDF = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", kafkaSourceTopic)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()

  // load avro schema from file
  val avroSchema = new String(Files.readAllBytes(Paths.get("../data/SampleData03/schema/invoice-items")))

  val valueDF = kafkaSourceDF.select(from_avro(col("value"), avroSchema).alias("value"))

  //valueDF.printSchema()

  val rewardsDF = valueDF.filter("value.CustomerType == 'PRIME'")
    .groupBy("value.CustomerCardNo")
    .agg(sum("value.TotalValue").alias("TotalPurchase"),
      sum(expr("value.TotalValue * 0.2").cast("integer")).alias("AggregatedRewards"))

  val kafkaTargetDF = rewardsDF.select(expr("CustomerCardNo as key"),
    to_json(struct("TotalPurchase", "AggregatedRewards"))
      .alias("value"))

  //kafkaTargetDF.show(false)

  val kafkaOutTopic = "kafka_avro_output"
  val rewardsWriterQuery = kafkaTargetDF
    .writeStream
    .queryName("Rewards Writer")
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", kafkaOutTopic)
    .outputMode("update")
    .option("checkpointLocation", "chk-point-dir")
    .option("failOnDataLoss", "false")
    .start()

  rewardsWriterQuery.awaitTermination()
}
