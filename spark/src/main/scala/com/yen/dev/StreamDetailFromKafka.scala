package com.yen.dev

//import org.apache.ivy.plugins.trigger.Trigger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.Trigger

// https://github.com/yennanliu/NYC_Taxi_Pipeline/blob/master/src/main/scala/KafkaEventLoad/LoadKafkaEventExample.scala

object StreamDetailFromKafka extends App{

  val sc = new SparkContext("local[*]", "StreamDetailFromKafka")

  val spark = SparkSession
    .builder
    .appName("StreamFromKafka")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
    .getOrCreate()

  import spark.implicits._

  // kafka config
  val bootStrapServers = "127.0.0.1:9092"
  val topic = "raw_data"

  // subscribe to topic
  val streamDF = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootStrapServers)
    .option("subscribe", topic)
    .load()

  val tmpStreamDF = streamDF.selectExpr( "CAST(topic AS STRING)", "CAST(value AS STRING)")

  tmpStreamDF.printSchema

  val StreamDFSource = tmpStreamDF
    .select("topic", "value")

  StreamDFSource.createOrReplaceTempView("event_table")

  //val query = "SELECT topic, value FROM event_table"
  val query = """
      |SELECT
      |topic,
      |split(value, ',')[0] AS runtime,
      |split(value, ',')[1] AS msg
      |FROM
      |event_table
      |""".stripMargin

  spark.sql(query)
    .writeStream
    .format("console")
    .trigger(Trigger.ProcessingTime("1 minute")) // how long trigger each batch stream // https://www.udemy.com/course/apache-spark-streaming-in-scala/learn/lecture/21955214#questions
    .start()
    .awaitTermination()
}
