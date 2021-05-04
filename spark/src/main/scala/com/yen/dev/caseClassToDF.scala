package com.yen.dev

/** Demo : transform scala case class to Spark Dataframe */

// https://github.com/yennanliu/NYC_Taxi_Pipeline/blob/master/src/main/scala/SaveToHive/SparkHiveExample.scala

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object caseClassToDF extends App {

  val spark = SparkSession
    .builder
    .appName(this.getClass.getName)
    .master("local[*]")
    .getOrCreate()

  // example 1
  case class myRecord(key: Int, value: String)
  val myRecordDF = spark.createDataFrame((1 to 20).map(i => myRecord(i, s"value-$i")))
  myRecordDF.show()
}
