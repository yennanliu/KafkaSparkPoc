package com.yen.sparkBatchBasics

import org.apache.spark.SparkContext

// https://www.youtube.com/watch?v=3Ygse23Ovds&list=PLmOn9nNkQxJF-qlCCDx9WsdAe6x5hhH77&index=46

/**
 *   Join
 *
 *   1) will implement on (K,V), (K,W) RDD
 *      -> will return a RDD with keys that contains all its corresponding elements
 *      -> e.g. (K,(V,V)) RDD
 *
 *   2) NOTE : in scala, `Option` means can have value or not
 *           -> if has value : Some(value)
 *           -> if has NO value : None
 */


object JoinDemo1 extends App {

  val sc = new SparkContext("local[*]", "JoinDemo1")

  val rdd1 = sc.parallelize(Array((1,"a"),(2,"b"),(3,"c")))

  val rdd2 = sc.parallelize(Array((1,4),(2,5),(4,6)))

  // join
  val rdd_1_2 = rdd1.join(rdd2) // inner join actually

  println(rdd_1_2.collect().toList) // List((1,(a,4)), (2,(b,5)))

  println("====================")

  println(rdd1.leftOuterJoin(rdd2).collect().toList) // List((1,(a,Some(4))), (2,(b,Some(5))), (3,(c,None)))

  println("====================")

  println(rdd1.rightOuterJoin(rdd2).collect().toList) // List((1,(Some(a),4)), (2,(Some(b),5)), (4,(None,6)))

  println("====================")

  println(rdd1.fullOuterJoin(rdd2).collect().toList) // List((1,(Some(a),Some(4))), (2,(Some(b),Some(5))), (3,(Some(c),None)), (4,(None,Some(6))))
}
