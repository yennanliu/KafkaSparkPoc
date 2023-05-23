package com.yen.poc1

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

object test1 extends App {

  val spark = SparkSession
    .builder
    .appName(this.getClass.getName)
    .master("local[*]")
    .getOrCreate()

  // load from s3
  val s3_bucket = "s3://xxx/yyy"
  val nested: sql.DataFrame = spark.read.option("multiline", true).json(s3_bucket)

  nested.printSchema

  // write to redshift

}
