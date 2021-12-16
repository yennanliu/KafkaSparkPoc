package com.yen.dev

// https://spark.apache.org/docs/latest/streaming-programming-guide.html

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object demo1_newApi extends App{

  val conf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount")
  // StreamingContext is entry point to spark stream
  val ssc = new StreamingContext(conf, Seconds(1))

  // Create a DStream that will connect to hostname:port, like localhost:9999
  val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

  // Split each line into words, words is DStream[String] type
  val words:DStream[String] = lines.flatMap(_.split(" "))

  // mapping
  val pairs = words.map(word => (word, 1))

  // reduceByKey
  val wordCounts = pairs.reduceByKey(_ + _)

  wordCounts.print()

  ssc.start()             // Start the computation
  ssc.awaitTermination()  // Wait for the computation to terminate
}
