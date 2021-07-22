package com.yen.sparkBatchBasics

import org.apache.spark.SparkContext

// https://www.youtube.com/watch?v=hBIApG_G3mI&list=PLmOn9nNkQxJF-qlCCDx9WsdAe6x5hhH77&index=49

object Action1 extends App {

  val sc = new SparkContext("local[*]", "Action1")

  /**
   * def aggregate[U : ClassTag](zeroValue: U)(seqOp: (U, T) => U,combOp: (U, U) => U)
   *
   *  so
   *    in rdd1.aggregate(1)(_+_, _+_) case,
   *    zeroValue will +1
   *    seqOp will + 1
   */

  val rdd1 = sc.makeRDD(1 to 4,2)

  println(rdd1.aggregate(0)(_+_, _+_)) // 10

  println(rdd1.aggregate(1)(_+_, _+_)) // 13

  println("================")

  val rdd2 = sc.makeRDD(1 to 4,4)

  println(rdd2.aggregate(1)(_+_, _+_)) // 15

  println("================")

  // compare : fold

  println(rdd1.fold(0)(_+_)) // 10

  println(rdd1.fold(1)(_+_)) // 13
}
