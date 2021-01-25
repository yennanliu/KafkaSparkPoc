package com.yen.dev

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.col
import org.apache.spark.sql.functions._

object ParseTxt2Json extends App{

  val sc = new SparkContext("local[*]", "LoadGreenTripData")

  //val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  val spark = SparkSession
    .builder
    .appName("SparkBatchTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val parseTxt = spark
    .read
    .textFile("../data/dev/parsed.txt")

  val parseRDD = parseTxt
      .map(x => x.split("\t"))
      .map(x => x(1))

  val parseDF = parseRDD.toDF()

  // https://stackoverflow.com/questions/39255973/split-1-column-into-3-columns-in-spark-scala
  val splitDF = parseDF.withColumn("_tmp", split($"value", ",")).select(
    $"_tmp".getItem(0).as("col1"),
    $"_tmp".getItem(1).as("col2"),
    $"_tmp".getItem(2).as("col3")
  )

  println("*******")
  println(splitDF.columns)
  println(splitDF.show(false))
  println("*******")
}
