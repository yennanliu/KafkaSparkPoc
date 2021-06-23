package com.yen.sparkBatchBasics

// https://www.youtube.com/watch?v=_2uXpMkggw0&list=PLmOn9nNkQxJF-qlCCDx9WsdAe6x5hhH77&index=36

import org.apache.spark.SparkContext

object sample1 extends App {

  val sc = new SparkContext("local[*]", "sample1")

  // demo 1
  val rdd1 = sc.parallelize(Array(1,2,3,4))

  val sample1 = rdd1.sample(true, 0.4, 2).collect()

  println("sample1 = " + sample1.toList)

  // demo 2
  val rdd2 = sc.parallelize(1 to 30)

  val sample2 = rdd2.sample(false, 0.3, 3).collect()

  println("sample2 = " + sample2.toList)
}
