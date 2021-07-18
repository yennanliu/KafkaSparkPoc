package com.yen.sparkBatchBasics

import org.apache.spark.SparkContext

// https://www.youtube.com/watch?v=3Ygse23Ovds&list=PLmOn9nNkQxJF-qlCCDx9WsdAe6x5hhH77&index=46

/**
 *   MapValues
 *
 *   1) implement op ONLY ON v  ( (k,v) pair )
 */

object MapValues1 extends App {

  val sc = new SparkContext("local[*]", "MapValue1")

  val rdd1 = sc.parallelize(Array((1,"dd"),(2,"cc"),(3,"ss")))

  // mapValues -> op on each element
  val rdd2 = rdd1.mapValues(x => x + "????")

  println(rdd2.collect().toList) // List((1,dd????), (2,cc????), (3,ss????))

  println("==================")

  // compare with map
  val rdd2_ = rdd1.map(x => x+ "????")

  println(rdd2_.collect().toList)  // List((1,dd)????, (2,cc)????, (3,ss)????)

  println("==================")

  val rdd2_2 = rdd1.map(x => (x._1, x._2 + "????"))
  println(rdd2_2.collect().toList)  // List((1,dd????), (2,cc????), (3,ss????))
}
