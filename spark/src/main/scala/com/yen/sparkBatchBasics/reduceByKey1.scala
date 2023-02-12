package com.yen.sparkBatchBasics

import org.apache.spark.SparkContext

// https://www.youtube.com/watch?v=v-n85y2YTR0&list=PLmOn9nNkQxJF-qlCCDx9WsdAe6x5hhH77&index=41
// https://github.com/yennanliu/til/blob/master/README.md#20230212
/**
 *  reduceByKey
 *
 *  1) operates on pair RDD ( k, v RDD)
 *  2) operates on value (v) under key (k), then aggregate
 *
 *  3) definition :
 *  def reduceByKey(func: (V, V) => V): RDD[(K, V)] = self.withScope {
 *     reduceByKey(defaultPartitioner(self), func)
 *     }
 *
 */


object reduceByKey1 extends App {

  val sc = new SparkContext("local[*]", "reduceByKey1")

  val rdd1 = sc.parallelize(List(("a", 1), ("b", 5), ("c", 1), ("a", 2), ("b", 1)))

  // demo 1 : get elements count via reduceByKey
  val r1 = rdd1.reduceByKey( (x,y) => x + y).collect()
  println ("r1 = " + r1.toList)

  println("=================")

  // demo 2 : reduce in scala
  val array1 = Array(1,2,3)
  val array1_reduced = array1.reduce((x,y) => 1)
  println("array1_reduced = " + array1_reduced)
}
