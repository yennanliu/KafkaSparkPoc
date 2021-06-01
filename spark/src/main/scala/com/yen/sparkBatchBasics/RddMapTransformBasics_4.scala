package com.yen.sparkBatchBasics

import org.apache.spark.SparkContext
import org.apache.spark.rdd.{ PairRDDFunctions, RDD }

object RddMapTransformBasics_4 extends App {

  val sc = new SparkContext("local[*]", "RddMapTransformBasics_4")

  println("============ example 1 ============")
  // Array -> case class

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

  println("============ example 2 ============")
  // case class -> RDD
  // https://stackoverflow.com/questions/34904354/how-do-i-put-a-case-class-in-an-rdd-and-have-it-act-like-a-tuplepair

  val rdd1 = sc.parallelize(data1)

  val rdd1_ = rdd1.map{ x =>
                val tmp = x match {
                case Array(x: String, y: Long, z: String) => myEvent(x,y,z)
                case _ => myEvent("", 100L, "")
              }
               tmp
            }

  rdd1_.foreach(println(_))

  println("============ example 3 ============")
  // case class -> RDD
  // https://stackoverflow.com/questions/34904354/how-do-i-put-a-case-class-in-an-rdd-and-have-it-act-like-a-tuplepair

  case class Foo(k: String, v1: String, v2: String)

  implicit def fooToPairRDDFunctions[K, V](rdd: RDD[Foo]): PairRDDFunctions[String, (String, String)] =
    new PairRDDFunctions(
      rdd.map {
        case Foo(k, v1, v2) => k -> (v1, v2)
      }
    )

  val rdd3 = sc.parallelize(List(Foo("a", "b", "c"), Foo("d", "e", "f")))

  rdd3.foreach(println(_))
}
