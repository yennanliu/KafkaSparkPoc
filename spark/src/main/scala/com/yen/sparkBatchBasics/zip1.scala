package com.yen.sparkBatchBasics

import org.apache.spark.{SparkConf, SparkContext}

// https://www.youtube.com/watch?v=v3ZWFwm57Ng&list=PLmOn9nNkQxJF-qlCCDx9WsdAe6x5hhH77&index=39

/**
 *   Zip operation in spark
 *
 *   1) rdd1.zip(rdd2) : rdd1 as key, rdd2 as value
 *   2) rdd2.zip(rdd1) : rdd2 as key, rdd1 as value
 *   3) can only zip "same elements count" and "same partition"
 *      -> check examples below
 *   4) Note : scala has its own zip op as well
 *
 */

object zip1 extends App {

  val sc = new SparkContext("local[*]", "zip1")

  val rdd1 = sc.parallelize(Array(1,2,3))
  val rdd2 = sc.parallelize(Array("a", "b", "c"))

  // demo 1
  println(rdd1.zip(rdd2).collect().toList)

  println("=================")

  // demo 2
  val rdd3 = sc.parallelize(Array("a", "b", "c"), 4)

  // will have exception below (unequal numbers of partitions)
  // Exception in thread "main" java.lang.IllegalArgumentException: Can't zip RDDs with unequal numbers of partitions: List(4, 8)
  //println(rdd3.zip(rdd1).collect().toList)

  println("=================")

  // demo 3
  val rdd4 = sc.parallelize(Array(1,2,3,4))
  // exception below
  // Caused by: org.apache.spark.SparkException: Can only zip RDDs with same number of elements in each partition
  //println(rdd4.zip(rdd2).collect().toList)
}
