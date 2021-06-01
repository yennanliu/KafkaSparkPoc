package com.yen.sparkBatchBasics

// https://dzone.com/articles/spark-transformations-for-pair-rdd

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD._

object RddMapTransformBasics_1 extends App {

  val sc = new SparkContext("local[*]", "RddMapReduceDemo1")

  println("=======  demo 1 : create key, value for RDD ============")
  val data1 = List("a","b","c","a","b","x")
  val rdd1 = sc.parallelize(data1)
  val pairRdd1 = rdd1.map(x => (x,1))

  pairRdd1.foreach(println(_))

  println("=======  demo 2 : groupByKey ============")
  /**
   * groupByKey
   *
   * This transformation groups all the rows with the same key into a single row.
   * The number of rows in the resulting RDD will be the same as the number
   * of rows of unique keys in the input RDD.
   */
  val rdd2 = sc.parallelize(List("hello","world","good","morning", "good"))
  val pairRdd2 = rdd2.map(x => (x,1))
  val r2 = pairRdd2.groupByKey()
  r2.foreach(println(_))

  println("=======  demo 3 : reduceByKey ============")
  /**
   *  reduceByKey
   *  - This transformation reduce all the values of the same key to a single value.
   *
   *  This process performs into two steps.
   *  1) Group the values of the same key.
   *  2) Apply the reduce function to the list of values of each key.
   */
  val rdd3 = sc.parallelize(List("hello","world","good","morning", "good"))
  val pairRdd3 = rdd3.map(x => (x,1))
  val r3 = pairRdd3.reduceByKey((x,y) => x + y)
  r3.foreach(println(_))
}
