package com.yen.sparkBatchBasics

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD._

import scala.util.Random

object RddMapTransformBasics_2 extends App {

  val sc = new SparkContext("local[*]", "RddMapTransformBasics_2")

  case class myEvent (
    id: String,
    timestamp: Long,
    msg: String
  )

  val event1 = myEvent("001", System.currentTimeMillis(), "hello !!!")
  val event2 = myEvent("001", System.currentTimeMillis() + Random.nextInt(), "world ~~~~")
  val event3 = myEvent("002", System.currentTimeMillis() + Random.nextInt() , "?? xxx ttt")
  val event4 = myEvent("003", System.currentTimeMillis() + Random.nextInt(), "errrrrr")

  val events = List(event1,event2, event3, event4)

  val rdd1 = sc.parallelize(events)
  rdd1.foreach(println(_))

  println("============ example 1 ============")

  val pairRdd1 = rdd1.map{x =>
    val tmp = (x.id, 1)
    tmp
  }
  val r1 = pairRdd1.reduceByKey((x,y) => x + y)
  r1.foreach(println(_))


  println("============ example 2 ============")

  val pairRdd2 = rdd1
    .filter(x => x.id == "001")
    .map{x =>
    val tmp = ( (x.id, x.msg), 1)
    tmp
  }

  val r2 = pairRdd2.reduceByKey((x,y) => x + y)
  r2.foreach(println(_))
}
