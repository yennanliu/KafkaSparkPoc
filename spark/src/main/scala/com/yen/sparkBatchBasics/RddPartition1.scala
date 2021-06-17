package com.yen.sparkBatchBasics

import org.apache.spark.SparkContext

// https://www.youtube.com/watch?v=IaMI70rlPvo&list=PLmOn9nNkQxJF-qlCCDx9WsdAe6x5hhH77&index=30

object RddPartition1 extends App {

  /**
   *   Spark parallelism
   *
   *   if # of core = parallelism ->  parallelism = parallelism
   *   if # of core = parallelism ->  parallelism = # of core
   */

  println("RddPartition1 start ...")

  val sc = new SparkContext("local[*]", "RddPartition1")

  val rdd1 = sc.parallelize(Array(1,2,3,4))
  val rdd1_ = sc.makeRDD(Array(1,2,3,4))

  println("rdd1.collect() = " + rdd1.collect())
  println("rdd1_.collect() = " + rdd1_.collect())

  println("rdd1.partitions = " + rdd1.partitions)
  println("rdd1.partitions.length = " + rdd1.partitions.length)  // default value : how many CPU the machine has

  println("==================")

  val rdd2 = sc.parallelize(Array(1,2,3,4), 4)

  println("rdd2.partitions.length = " + rdd2.partitions.length) // 4

  println("RddPartition1 end ...")
}
