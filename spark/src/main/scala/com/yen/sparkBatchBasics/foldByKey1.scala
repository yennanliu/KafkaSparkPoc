package com.yen.sparkBatchBasics

import org.apache.spark.SparkContext

// https://www.youtube.com/watch?v=n7B3a8VbVyE&list=PLmOn9nNkQxJF-qlCCDx9WsdAe6x5hhH77&index=43

/**
 * foldByKey
 *
 * General ordering :
 *    aggregateByKey -> foldByKey -> reduceByKey
 *
 */

object foldByKey1 extends App {

  val sc = new SparkContext("local[*]", "foldByKey1")

  val rdd1 = sc.parallelize(List( ("a",3), ("a",2), ("c",4), ("b",3), ("b",6), ("c",8) ), 2)


  // wordcount : fold by key
  val r1 = rdd1.foldByKey(0)((x,y) => x + y).collect()
  val r1_ = rdd1.foldByKey(0)(_+_).collect()

  println ("r1 = " + r1.toList)
  println ("r1_ = " + r1_.toList)


}
