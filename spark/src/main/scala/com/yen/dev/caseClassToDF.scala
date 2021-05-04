package com.yen.dev

/** Demo : transform scala case class to Spark Dataframe */

// https://github.com/yennanliu/NYC_Taxi_Pipeline/blob/master/src/main/scala/SaveToHive/SparkHiveExample.scala

import java.util.Date

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

  // example 2
  case class myRecord2(key: Int, value: String, runtime: String)
  val myRecordDF2 = spark.createDataFrame((1 to 20).map(i => myRecord2(i, s"value-$i", new Date().getTime().toString)))
  myRecordDF2.show()

  // example 3
  case class myRecord3(key: Int, value: String, runtime: String, msg: String)
  val myRecordDF3 = spark.createDataFrame((1 to 20).map(
    i => myRecord3(i,
      s"value-$i",
      new Date().getTime().toString,
      i match {
        case _ if i % 2 == 0 => "even"
        case _ => "odd"
      })))
  myRecordDF3.show()
}
