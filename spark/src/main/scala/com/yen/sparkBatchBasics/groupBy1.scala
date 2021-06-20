package com.yen.sparkBatchBasics

// https://www.youtube.com/watch?v=cIMCJkGSJkc&list=PLmOn9nNkQxJF-qlCCDx9WsdAe6x5hhH77&index=34

import org.apache.spark.SparkContext

/**
 *  groupBy
 *
 *  1) group elements based on input func
 *  2) will put return value with same key in the SAME iterator
 *
 *  def groupBy[K](f: T => K)(implicit kt: ClassTag[K]): RDD[(K, scala.Iterable[T])
 */

object groupBy1 extends App {

  println("groupBy1 start ...")

  val sc = new SparkContext("local[*]", "groupBy1")

  val rdd1 = sc.parallelize(Array(1,2,3,4), 2) // force split RDD to 2 partition

  // groupBy demo 1
  val r1 = rdd1.groupBy(x => x % 2).collect()

  println("r1 = " + r1.toList)


  // groupBy demo 2
  val r2 = rdd1.groupBy(
    x => x match {
      case _ if x == 1 => 1
      case _ if x == 2 => 2
      case _ => 0
    }
  ).collect()

  println("r2 = " + r2.toList)

  // groupBy demo 3
  val rdd3 = sc.parallelize(Array("tom", "jim", 1,2,3, "kim"))

  val r3 = rdd3.groupBy(
    x => x match {
      case x:String if x == "tom" || x == "jim" => "jim"
      case x:Int => "others"
      case _ => "kim"
    }
  ).collect()

  println("r3 = " + r3.toList)

  println("groupBy1 end ...")
}
