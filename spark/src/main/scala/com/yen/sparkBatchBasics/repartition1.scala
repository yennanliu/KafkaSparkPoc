package com.yen.sparkBatchBasics

import org.apache.spark.SparkContext

// https://www.youtube.com/watch?v=_2uXpMkggw0&list=PLmOn9nNkQxJF-qlCCDx9WsdAe6x5hhH77&index=36

object repartition1 extends App {

  val sc = new SparkContext("local[*]", "repartition1")

  // demo 1
  val rdd1 = sc.parallelize(1 to 30)

  /**
   * mapPartitionsWithIndex
   *
   * // (i, items)
   * // i : partition index
   * // items : elements in partition, an iterator
   */
  val r1 = rdd1
    .mapPartitionsWithIndex( (i, items) => items.map((i, _)) )
    .collect()

  println("rdd1.mapPartitionsWithIndex = " + r1.toList)

  /**
   * repartition
   *
   * -> re-define partition, and repartition the data with that
   * (can enlarge, lower partition)
   */
  val r2 = rdd1
    .repartition(2)
    .mapPartitionsWithIndex( (i, items) => items.map((i, _)))
    .collect()

  println("rdd1.repartition(2).mapPartitionsWithIndex = " + r2.toList)

  /**
   * coalesce
   *
   * -> reduce partition counts, will enhance efficiency if small data set
   * -> can choose if "shuffle"
   */
  val r3 = rdd1
    .coalesce(2)
    .mapPartitionsWithIndex( (i, items) => items.map((i, _)))
    .collect()

  println("rdd1.coalesce(2).mapPartitionsWithIndex = " + r3.toList)

  // repartition VS coalesce
  // coalesce -> can choose if "shuffle"
  // repartition calls coalesce actually, but with shuffle = true
  val rdd2 = sc.parallelize(Array(1,2,3,4,7,8))
  println(rdd2.coalesce(6).partitions.length)
  println(rdd2.coalesce(6, true).partitions.length)
  println("rdd1.repartition(9).partitions.length = " + rdd1.repartition(9).partitions.length)
}
