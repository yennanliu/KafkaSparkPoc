package com.yen.sparkBatchBasics

import org.apache.spark.SparkContext

object RddMapTransformBasics_4 extends App {

  val sc = new SparkContext("local[*]", "RddMapTransformBasics_4")

  case class myEvent (
                       id: String,
                       timestamp: Long,
                       msg: String
                     )

  val data1 = Array("001", System.currentTimeMillis(), "hello !!!")

  // transform Array to case class obj
  // https://stackoverflow.com/questions/4290955/instantiating-a-case-class-from-a-list-of-parameters?rq=1
  val data1_ = data1 match {
    case Array(x: String, y: Long, z: String) => myEvent(x,y,z)
  }

  println("data1_ = " + data1_)

  //val rdd1 = sc.parallelize(data1_)
}
