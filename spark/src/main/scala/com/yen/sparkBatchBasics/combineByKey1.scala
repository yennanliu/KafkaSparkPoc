package com.yen.sparkBatchBasics

import org.apache.spark.SparkContext

/**
 *  combineByKey
 *
 *
 *
 *
 *  // definition :  NOTE : we need to offer param type when use combineByKey
 *
 *  def combineByKey[C](
 *    createCombiner: V => C,
 *    mergeValue: (C, V) => C,
 *    mergeCombiners: (C, C) => C): RDD[(K, C)] = self.withScope {
 *    combineByKeyWithClassTag(createCombiner, mergeValue, mergeCombiners)(null)
 *   }
 *
 */

// https://www.youtube.com/watch?v=XHQvhuGLXeg&list=PLmOn9nNkQxJF-qlCCDx9WsdAe6x5hhH77&index=44

object combineByKey1 extends App {

  val sc = new SparkContext("local[*]", "combineByKey1")

  /** demo 1: create a pairRDD,  get the avg value based on key */

  val rdd1 = sc.parallelize(Array(("a",88), ("b",95), ("a",91), ("b", 93), ("b", 98)))

  // ----------------------------
  // Step 1 : get sum per key
  // ----------------------------

  // wrong
  //val r1 = rdd1.combineByKey(v => (v,1), (c,v)=>(c._1+v, c._2+1), (c1,c2) => (c1._1 + c2._1, c1._2 + c2._2))

  // correct
  val r1 = rdd1.combineByKey(
     (_,1), (acc:(Int, Int),v) => (acc._1 + v, acc._2 +1 ),
     (acc1:(Int,Int), acc2:(Int,Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
  )

  println("r1 = " + r1.collect().toList)

  // ----------------------------
  // Step 2 : get average per key
  // ----------------------------

  val r1_ = r1.map(x => (x._1, x._2._1 / x._2._2 ))
  println("r1_ = " + r1_.collect().toList)

  // or, you can get result as double type
  val r1_2 = r1_.map{ case (k,v) => (k, v.toDouble) }

  println("r1_2 = " + r1_2.collect().toList)
}
