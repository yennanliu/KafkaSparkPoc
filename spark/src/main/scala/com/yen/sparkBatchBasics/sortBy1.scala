package com.yen.sparkBatchBasics

import org.apache.spark.SparkContext

// https://www.youtube.com/watch?v=9aAi1DI6kvI&list=PLmOn9nNkQxJF-qlCCDx9WsdAe6x5hhH77&index=37

object sortBy1 extends App {

  val sc = new SparkContext("local[*]", "sortBy1")

  // example 1
  val rdd1 = sc.parallelize(Array(1,2,3,4,5))

  // sort by x => x
  rdd1.sortBy(x => x).collect().foreach(println(_))

  println("===========")

  // sort by x => - x
  rdd1.sortBy(x => -x).collect().foreach(println(_))

  val rdd2 = sc.parallelize(Array("aaa", "b", "cccccc", "dd"))

  println("===========")

  // sort by x => x.size
  rdd2.sortBy(x => x.size).collect().foreach(println(_))
}
