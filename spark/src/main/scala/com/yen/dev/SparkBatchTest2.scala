package com.yen.dev

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import com.yen.adaptor.SparkBatchTest2Factory

object SparkBatchTest2 extends App {
  println("SparkBatchAdaptorTest start ...")

  val sc = new SparkContext("local[*]", "LoadGreenTripData")

  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  val spark = SparkSession
    .builder
    .appName("SparkBatchAdaptorTest")
    .master("local[*]")
    .getOrCreate()

  val sampleRDD = sc.parallelize(
    Seq(
      ("first", Array(2.0, 1.0, 2.1, 5.4)),
      ("test", Array(1.5, 0.5, 0.9, 3.7)),
      ("choose", Array(8.0, 2.9, 9.1, 2.5))
    )
  )

  // run
  //val sampleDF = spark.createDataFrame(sampleRDD)
  val sampleDF = SparkBatchTest2Factory(spark,sampleRDD)
  sampleRDD.collect()
  sampleDF.show()

  println("SparkBatchAdaptorTest end ...")
}
