package com.yen.sparkBatchBasics

import org.apache.spark.SparkContext

// https://www.youtube.com/watch?v=p1nDboACJfA&list=PLmOn9nNkQxJF-qlCCDx9WsdAe6x5hhH77&index=45

/**
 *  combineByKey for word count
 */

object combineByKey2 extends App {

  val sc = new SparkContext("local[*]", "combineByKey2")

  val rdd1 = sc.parallelize(List( ("a",3), ("a",2), ("c",4), ("b",3), ("b",6), ("c",8) ), 2)

  // word count
  val r1 = rdd1.combineByKey(v => v, (c:Int, v) => c+v, (c1:Int, c2:Int)=> c1+c2).collect()

  println("r1 = " + r1.toList)
}
