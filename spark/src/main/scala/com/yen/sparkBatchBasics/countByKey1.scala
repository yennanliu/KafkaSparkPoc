package com.yen.sparkBatchBasics

import org.apache.spark.SparkContext

// https://www.youtube.com/watch?v=eaKHArSYwcA&list=PLmOn9nNkQxJF-qlCCDx9WsdAe6x5hhH77&index=50

object countByKey1 extends App {

  val sc = new SparkContext("local[*]", "countByKey1")

  val rdd1 = sc.parallelize(List((1,3),(3,2),(1,5),(6,7)))

  println(rdd1.countByKey()) // Map(1 -> 2, 3 -> 1, 6 -> 1)
}
