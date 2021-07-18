package com.yen.sparkBatchBasics

import org.apache.spark.SparkContext

// https://www.youtube.com/watch?v=3Ygse23Ovds&list=PLmOn9nNkQxJF-qlCCDx9WsdAe6x5hhH77&index=46

/**
 *   Join
 *
 *   1) will implement on (K,V), (K,W) RDD
 *      -> will return a RDD with keys that contains all its corresponding elements
 *      -> e.g. (K,(V,V)) RDD
 */


object JoinDemo1 extends App {

  val sc = new SparkContext("local[*]", "JoinDemo1")

  val rdd1 = sc.parallelize(Array((1,"a"),(2,"b"),(3,"c")))

  val rdd2 = sc.parallelize(Array((1,4),(2,5),(3,6)))

  // join

}
