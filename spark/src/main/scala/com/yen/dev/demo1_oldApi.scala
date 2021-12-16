package com.yen.dev

import org.apache.spark.SparkConf
import org.apache.spark.streaming.api.java.{JavaDStream, JavaReceiverInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

// https://spark.apache.org/docs/2.2.0/streaming-programming-guide.html#discretized-streams-dstreams

object demo1_oldApi extends App {

  val conf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount")
  // StreamingContext is entry point to spark stream
  //val ssc = new StreamingContext(conf, Seconds(1))

  // The existing application is shutdown gracefully (see StreamingContext.stop(...) or JavaStreamingContext.stop(...) for graceful shutdown options) which ensure data that has been received is completely processed before shutdown.
  val ssc = new JavaStreamingContext(conf, Seconds(1))

  val lines: JavaReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

//
//  def mySplitFunc(input:String):String={
//    input.split("").toString
//  }
//
//  //val words: JavaDStream[String] = lines.flatMap(x => mySplitFunc(x))
//
//  val wordCounts = words
//    .map(word => (word, 1))
//    .reduceByKey(_ + _)
//
//  val cleanedDStream = wordCounts.transform { rdd =>
//    rdd.filter(x=> x!= "1")
//  }
//
//  cleanedDStream.print()

  ssc.start()
  ssc.awaitTermination()
}
