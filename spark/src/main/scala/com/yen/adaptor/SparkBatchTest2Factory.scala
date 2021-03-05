package com.yen.adaptor

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

case class SparkBatchTest2Factory()

object SparkBatchTest2Factory {
  //import spark.implicits._
  def apply(sparkSession: SparkSession, rdd: RDD[(String, Array[Double])]):DataFrame = {
    val df = sparkSession.createDataFrame(rdd)
    df
  }
}
