package com.yen.sparkBatchBasics

import org.apache.spark.SparkContext

// https://www.youtube.com/watch?v=v-n85y2YTR0&list=PLmOn9nNkQxJF-qlCCDx9WsdAe6x5hhH77&index=41

/**
 *  groupByKey1
 *
 *  1) group RDD with key, return a "key + Iterable" object for following operation (e.g. reduce, collect ...)
 *
 *  2) *** ReduceByKey VS groupByKey
 *     2-1) ReduceByKey
 *        -> do op based on key, then combine then shuffle, the aggregate, -> return RDD[k,v]
 *        -> ReduceByKey = map + reduce
 *    2-2) groupByKey
 *        -> group based on key (with shuffle) directly, return RDD[k,Iterable(v)]
 *        -> groupByKey = step between map and reduce
 *
 *  3) ReduceByKey is more preferable than groupByKey in general cases
 *
 *  4) definition :
 *   def groupByKey(): RDD[(K, Iterable[V])] = self.withScope {
 *     groupByKey(defaultPartitioner(self))
 *    }
 */


object groupByKey1 extends App {

  val sc = new SparkContext("local[*]", "reduceByKey1")

  val rdd1 = sc.parallelize(Array("a","b","c","a","b"))

  // create a pair RDD
  val rdd1Paired = rdd1.map(x => (x,1))

  // demo 1 : aggregate value with same key
  val r1 = rdd1Paired.groupByKey().collect()
  println("r1 = " + r1.toList) // Array( (a,CompactBuffer(1, 1)), (b,CompactBuffer(1, 1)), (c,CompactBuffer(1)) )

  println("=================")
}
