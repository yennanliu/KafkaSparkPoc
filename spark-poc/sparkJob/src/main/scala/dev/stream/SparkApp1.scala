package dev.stream

import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kinesis.{KinesisInitialPositions, KinesisInputDStream}
import org.apache.spark.streaming._
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kinesis.KinesisInitialPositions.Latest
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
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
    val numStreams = 100

    val conf = new SparkConf()
      .setAppName(APP_NAME)
      .setMaster("local[*]")


    val ssc = new StreamingContext(conf, Seconds(1))

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
      KinesisInputDStream.builder
        .streamingContext(ssc)
        .streamName(KINESIS_STREAM_NAME)
        .endpointUrl(KINESIS_END_POINT_URL)
        .regionName(REGION_NAME)
        .initialPosition(new Latest())
        .checkpointAppName(KINESIS_APP_NAME)
        //.checkpointInterval(new Duration(2000))
        .storageLevel(StorageLevel.MEMORY_AND_DISK_2)
        .build()
    }

    // Union all the streams
    val unionStreams = ssc.union(kinesisStreams)

    // Convert each line of Array[Byte] to String, and split into words
    val words = unionStreams.flatMap(byteArray => new String(byteArray).split(" "))

    // Map each word to a (word, 1) tuple so we can reduce by key to count the words
    val wordCounts = words.map(word => (word, 1)).reduceByKey(_ + _)
    val wordSample = words.map(word => (word, 1))

    //println(">>> count = " + wordCounts.count().toString)
    //println(">>> count = " + words.co)

    // Print the first 10 wordCounts
    wordCounts.print()
    wordSample.print()

    // Start the streaming context and await termination
    ssc.start()
    ssc.awaitTermination()

  }

}
