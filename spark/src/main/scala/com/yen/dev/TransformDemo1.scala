package com.yen.dev

// https://www.youtube.com/watch?v=Ezy6aaRG8hc&list=PLmOn9nNkQxJF-qlCCDx9WsdAe6x5hhH77&index=103

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TransformDemo1 extends App {

  // get sparkConf
  val sparkConf: SparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName(this.getClass.getName)

  // get StreamingContext
  val ssc = new StreamingContext(sparkConf, Seconds(3))

  // get DStream
  val lineDStream: ReceiverInputDStream[String] =
    ssc.socketTextStream("localhost", 9999)

  /** Transform */
  //*** transform DStream to RDD
  // (below is "non status" transformation
  val wordAndCountDStream: DStream[(String, Int)] = lineDStream.transform( rdd => {

    val words: RDD[String] = rdd.flatMap(_.split(" "))

    val wordAndOne: RDD[(String, Int)] = words.map((_, 1))

    val value: RDD[(String, Int)] = wordAndOne.reduceByKey(_+_)

    value
    }
  )

  wordAndCountDStream.print

  // run
  ssc.start()
  ssc.awaitTermination()
}
