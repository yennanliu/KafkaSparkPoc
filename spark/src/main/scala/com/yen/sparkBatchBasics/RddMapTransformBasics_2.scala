package com.yen.sparkBatchBasics

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object RddMapTransformBasics_2 extends App {

  val sc = new SparkContext("local[*]", "RddMapTransformBasics_2")

  val data1 = Array("a", "b", "c", "a", "b", "c", "x")
  val rdd1 = sc.parallelize(data1)

  rdd1.foreach(println(_))

  println("=======  demo 1 : transform RDD to k,v RDD, and reduceByKey ============")

  val pairRdd1 = rdd1.map(x => (x,1))
  val r1 = pairRdd1.reduceByKey((x,y) => x +y )
  r1.foreach(println(_))

  println("=======  demo 2 : redo above via methid ============")

  def func1(rdd: RDD[String]) = {
    val r = rdd.map(x => (x,1))
      .reduceByKey((x,y) => x + y)
    r
  }

  val r2 = func1(rdd1)
  r2.foreach(println(_))
}
