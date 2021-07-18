package com.yen.sparkBatchBasics

import org.apache.spark.SparkContext
;

// https://www.youtube.com/watch?v=3Ygse23Ovds&list=PLmOn9nNkQxJF-qlCCDx9WsdAe6x5hhH77&index=46

/**
 *   cogroup
 *
 *   1) implements on (K,V), (K,W) RDD
 *      -> will return a (K,(Iterable<V>,Iterable<W>)) type RDD
 *
 *   2) NOTE : the iterator in the returned RDD
 */

object coGroup1 extends App {

    val sc = new SparkContext("local[*]", "JoinDemo1")

    val rdd1 = sc.parallelize(Array((1,"a"),(2,"b"),(3,"c")))

    val rdd2 = sc.parallelize(Array((1,4),(2,5),(3,6)))

    val rdd3 = sc.parallelize(Array((1,"a"),(2,"b"),(3,"c"),(1,"e")))

    // cogroup

    val rdd1_ = rdd1.cogroup(rdd2)

    println(rdd1_.collect().toList) // List((1,(CompactBuffer(a),CompactBuffer(4))), (2,(CompactBuffer(b),CompactBuffer(5))), (3,(CompactBuffer(c),CompactBuffer(6)))

    println("===================")

    println(rdd3.cogroup(rdd2).collect().toList) // List((1,(CompactBuffer(a, e),CompactBuffer(4))), (2,(CompactBuffer(b),CompactBuffer(5))), (3,(CompactBuffer(c),CompactBuffer(6))))
}
