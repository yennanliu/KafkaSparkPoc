package com.yen.sparkBatchBasics

import org.apache.spark.SparkContext

/**
 *  combineByKey
 *
 *
 */

// https://www.youtube.com/watch?v=XHQvhuGLXeg&list=PLmOn9nNkQxJF-qlCCDx9WsdAe6x5hhH77&index=44

object combineByKey1 extends App {

  val sc = new SparkContext("local[*]", "combineByKey1")

  /** demo 1: reate a pairRDD,  get the avg value based on key */

  val rdd1 = sc.parallelize(Array(("a",88), ("b",95), ("a",91), ("b", 93), ("b", 98)))

  // wrong
  //val r1 = rdd1.combineByKey(v => (v,1), (c,v)=>(c._1+v, c._2+1), (c1,c2) => (c1._1 + c2._1, c1._2 + c2._2))

  // correct
  val r1 = rdd1.combineByKey( (_,1), (acc:(Int, Int),v) =>
  (acc._1+v, acc._2 +1 ), (acc1:(Int,Int), acc2:(Int,Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))

  println("r1 = " + r1.collect().toList)
}
