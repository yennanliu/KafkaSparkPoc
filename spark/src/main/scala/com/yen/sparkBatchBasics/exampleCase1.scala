package com.yen.sparkBatchBasics

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

// https://www.youtube.com/watch?v=aSz-Ll8-ncU&list=PLmOn9nNkQxJF-qlCCDx9WsdAe6x5hhH77&index=47

object exampleCase1 extends App {

  val sc = new SparkContext("local[*]", "exampleCase1")

  val lines: RDD[String] = sc.textFile("data.txt")

  // get city + ad : ((city,ad), 1)
  val cityAndAdToOne: RDD[((String, String), Int)] = lines.map( x => {
    val fields : Array[String] = x.split("")
    ((fields(1), fields(4)), 1)
    }
  )

  // ad total count by city :  ((city,ad), 16)
  val cityAndAdToCount: RDD[((String, String), Int)] =
  cityAndAdToOne.reduceByKey(_+_)

  // dimension trans. : (city,(ad, 1))
  val cityToAdAndCount: RDD[(String, (String, Int))] =
    cityAndAdToCount.map(
      x => (x._1._1,(x._1._2, x._2))
    )

  // aggregate on same city, but different ad :  (city,(ad, 1), (ad,12), (ad,13)...)
  val cityToAdGroup: RDD[(String, Iterable[(String, Int)])] =
    cityToAdAndCount.groupByKey()

  // ordering and get top 3
  val top3: RDD[(String, List[(String, Int)])] =
    cityToAdGroup.mapValues(
      x => { // NOTE : here we use scala way for sorting
        x.toList.sortWith((a,b) => a._2 > b._2)
          .take(3)
      }
    )

  // print the final result
  top3.collect().foreach(println(_))

  // close the sc
  sc.stop()
}
