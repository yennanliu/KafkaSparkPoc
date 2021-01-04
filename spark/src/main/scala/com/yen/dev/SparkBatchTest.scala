package com.yen.dev

// simple spark batch

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SparkSession}
// https://github.com/yennanliu/NYC_Taxi_Pipeline/blob/master/src/main/scala/DataLoad/LoadGreenTripData.scala

object SparkBatchTest extends App{

  val sc = new SparkContext("local[*]", "LoadGreenTripData")

  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  val spark = SparkSession
    .builder
    .appName("SparkBatchTest")
    .master("local[*]")
    .getOrCreate()

  println(s"*** sc : $sc")
  println(s"*** spark : $spark")

  val sampleRDD = sc.parallelize(Seq(1,2,3,4,5))

  val rCount = sampleRDD.count()

  val rCollect = sampleRDD.collect()

  println(s"sampleRDD.count() : $rCount")

  println(s"sampleRDD.collect() : $rCollect")

  val sampleRDD2 = sc.parallelize(
    Seq(
      ("first", Array(2.0, 1.0, 2.1, 5.4)),
      ("test", Array(1.5, 0.5, 0.9, 3.7)),
      ("choose", Array(8.0, 2.9, 9.1, 2.5))
    )
  )

  val sampleRDD2DF = spark.createDataFrame(sampleRDD2)

  sampleRDD2DF.show()
}
