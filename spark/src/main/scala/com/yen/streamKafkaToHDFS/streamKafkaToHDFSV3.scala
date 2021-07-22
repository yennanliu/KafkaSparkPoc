package com.yen.streamKafkaToHDFS

/**
 *  Spark stream process Kafka event WITH case class
 *  1) print in console
 *  2) sink (save) to HDFS WITH DIFFERENT TOPICS
 *  3) sink (save) to HDFS with bzip2 compression WITH DIFFERENT TOPICS
 *
 * // event source :
 * // https://github.com/yennanliu/KafkaSparkPoc/blob/main/kafka/src/main/scala/com/yen/Producer/producerV3.scala
 */

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object streamKafkaToHDFSV3 extends App {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  val spark = SparkSession
    .builder
    .appName(this.getClass.getName)
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
    .getOrCreate()

  import org.apache.spark.sql.functions._
  import spark.implicits._

  // kafka config
  val bootStrapServers = "127.0.0.1:9092"
  // digest event from a list of topics
  val topic = "raw_data_1,raw_data_2,raw_data_3"

  // subscribe to topic
  val streamDF = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootStrapServers)
    .option("subscribe",topic) // subscribe multiple topics : https://spark.apache.org/docs/2.2.0/structured-streaming-kafka-integration.html
    .load()

  val tmpStreamDF = streamDF.selectExpr(
    "topic", // select topic as col
    "CAST(key AS STRING)",
    "CAST(value AS STRING)"
  )

//  val eventDF = tmpStreamDF.select("value.*")
//    //.withColumn("CreatedTime", to_timestamp(col("CreatedTime"), "yyyy-MM-dd HH:mm:ss"))
//    .withColumn("id", expr("id"))
//    .withColumn("event_date", expr("event_date"))
//    .withColumn("msg", expr("msg"))

  val eventDF2 = tmpStreamDF.select($"*", explode($"value"))
    .withColumn("id", expr("id"))
    .withColumn("event_date", expr("event_date"))
    .withColumn("msg", expr("msg"))

  /** V1 : print out in console */
  val query = eventDF2.writeStream
            .format("console")
            .option("checkpointLocation", "chk-point-dir2")
            .start()

  tmpStreamDF.printSchema()

  /** V2 : save into HDFS */
//     val query = tmpStreamDF
//            .writeStream
//            .partitionBy("topic") // save data partition by column : http://spark.apache.org/docs/2.1.1/api/java/org/apache/spark/sql/streaming/DataStreamWriter.html
//            .format("json")
//            .option("checkpointLocation", "chk-point-dir2")
//            .option("path", s"streamKafkaToHDFSV3")
//            .start()

  query.awaitTermination()
}
