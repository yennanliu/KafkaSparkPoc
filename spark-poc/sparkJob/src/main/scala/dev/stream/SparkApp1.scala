package dev.stream

import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kinesis.{KinesisInitialPositions, KinesisInputDStream}
import org.apache.spark.streaming._
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.kinesis.KinesisInitialPositions.{Latest, TrimHorizon}
import org.apache.spark.streaming.kinesis.KinesisInitialPositions.Latest
import org.apache.spark.streaming.kinesis.KinesisInputDStream

/**
 * DEV app process Kinesis stream
 *
 * https://spark.apache.org/docs/latest/streaming-kinesis-integration.html
 * https://github.com/apache/spark/blob/master/connector/kinesis-asl/src/main/scala/org/apache/spark/examples/streaming/KinesisWordCountASL.scala
 *
 */
object SparkApp1 {

  def main(args: Array[String]): Unit = {

    val APP_NAME = "Stream SparkApp1"
    val KINESIS_END_POINT_URL = "kinesis.us-east-1.amazonaws.com"
    val REGION_NAME = "us-east-1"
    //val STREAM_NAME = "my_kinesis_stream_2"
    val KINESIS_APP_NAME = "KINESIS_APP"
    val KINESIS_STREAM_NAME = "my_kinesis_stream_2"
    val numStreams = 4

    val conf = new SparkConf()
      .setAppName(APP_NAME)
      .setMaster("yarn") // emr
      //.setMaster("local[*]") // local


    /**
     * SparkContext and StreamingContext co-exist in the same program
     * https://stackoverflow.com/questions/40623109/can-sparkcontext-and-streamingcontext-co-exist-in-the-same-program
     */
    val sc = new SparkContext(conf)

    val spark = SparkSession.builder
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    //val ssc = new StreamingContext(conf, Seconds(1))
    val ssc = new StreamingContext(sc, Seconds(1))


//    val kinesisStream = KinesisInputDStream.builder
//      .streamingContext(ssc)
//      .endpointUrl(END_POINT_URL)
//      .regionName(REGION_NAME)
//      .streamName(STREAM_NAME)
//      .initialPosition(KinesisInitialPositions.TrimHorizon)
//      .checkpointAppName(KINESIS_APP_NAME)
//      .checkpointInterval(new Duration(2000))
//      .metricsLevel(MetricsLevel.DETAILED)
//      .storageLevel(StorageLevel.MEMORY_AND_DISK_2)
//      .build()


    val kinesisStreams = (0 until numStreams).map { i =>

      println(">>> numStreams = " + numStreams)

      KinesisInputDStream.builder
        .streamingContext(ssc)
        .streamName(KINESIS_STREAM_NAME)
        .endpointUrl(KINESIS_END_POINT_URL)
        .regionName(REGION_NAME)
        //.initialPosition(new Latest())
        .initialPosition(new TrimHorizon())
        .checkpointAppName(KINESIS_APP_NAME)
        //.checkpointInterval(new Duration(2000))
        .storageLevel(StorageLevel.MEMORY_AND_DISK_2)
        .build()
    }

    //kinesisStreams

    // Union all the streams
    val unionStreams = ssc.union(kinesisStreams)

    //
    unionStreams.foreachRDD(rdd =>{
      if (rdd.count() > 0){
        println("record count = " + rdd.count())
        val str = rdd.map(str => new String(str))
        str.saveAsTextFile("/Users/yenanliu/KafkaSparkPoc/spark-poc/output/SparkApp1_output")

        str.saveAsTextFile("s3://spark-cluster-7/text-output/")

        val records = str.toString()
        val df = spark.read.json(Seq(records).toDS)

        // TODO : fix write to s3
        df.write
          .option("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
          .option("com.amazonaws.services.s3.enableV4", "true")
          .option("fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A")
          .mode("append")
          .csv("s3://spark-cluster-7/text-output/")

        df.count()
        df.show()
      }
    })

    println ("unionStreams count " + unionStreams.count())

    // Convert each line of Array[Byte] to String, and split into words
    val words = unionStreams.flatMap(byteArray => new String(byteArray).split(" "))
    println ("words count " + words.count())

    // Map each word to a (word, 1) tuple so we can reduce by key to count the words
    val wordCounts = words.map(word => (word, 1)).reduceByKey(_ + _)
    val wordSample = words.map(word => (word, 1))

    //unionStreams.print()

    //println(">>> count = " + wordCounts.count().toString)
    //println(">>> count = " + words.co)

    // Print the first 10 wordCounts
    wordCounts.print()
    wordSample.print()

    //unionStreams.saveAsTextFiles("/output/SparkApp1_output.txt")

    // Start the streaming context and await termination
    ssc.start()
    ssc.awaitTermination()
  }

}
