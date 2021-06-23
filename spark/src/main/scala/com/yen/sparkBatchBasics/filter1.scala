package com.yen.sparkBatchBasics

// https://www.youtube.com/watch?v=_2uXpMkggw0&list=PLmOn9nNkQxJF-qlCCDx9WsdAe6x5hhH77&index=36

import org.apache.spark.SparkContext

object filter1 extends App {

  val sc = new SparkContext("local[*]", "filter1")

  // demo 1
  val rdd1 = sc.parallelize(Array(1,2,3,4))

  val r1 = rdd1.filter(_ % 2 == 0).collect()

  val r1_ = rdd1.filter(x => (x % 2== 0)).collect()

  println("r1 = " + r1.toList)

  println("r1_ = " + r1_.toList)

  // demo 2
  val rdd2 = sc.parallelize(Array("tom", "tom kyo", "larce", "iioo" , "uuuu"))

  val r3 = rdd2.filter(x => (x.contains("tom"))).collect()

  println("r3 = " + r3.toList)
}
