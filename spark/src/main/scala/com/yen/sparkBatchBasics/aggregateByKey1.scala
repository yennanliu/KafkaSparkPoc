package com.yen.sparkBatchBasics

import org.apache.spark.SparkContext

// https://www.youtube.com/watch?v=T5dlY-Wgg2Q&list=PLmOn9nNkQxJF-qlCCDx9WsdAe6x5hhH77&index=43
// https://github.com/yennanliu/KafkaSparkPoc/blob/main/spark/doc/pic/aggregateByKey1.png

object aggregateByKey1 extends App {

  val sc = new SparkContext("local[*]", "aggregateByKey1")

  // NOTE : here we have 2 partition
  // so, partition1 = (("a",3), ("a",2), ("c",4)), partition2 = (("b",3), ("b",6), ("c",8))
  val rdd1 = sc.parallelize(List( ("a",3), ("a",2), ("c",4), ("b",3), ("b",6), ("c",8) ), 2)

  println(rdd1.collect().toList)

  println("=================")

  /**
   * demo 1 : aggregateByKey
   *
   *  1) format : rdd1.aggregateByKey(init value per key)(op inside partition, op within partition)
   *
   *  2) Steps :
   *
   *    2-1) init value for EACH key in EACH partition : 0
   *    2-2) in partition op : (u,v) => math.max(u,v)
   *         partition 1:
   *           -> for a : 0, max(0,3) = 3, max(3,2) = 3
   *           -> for c : 0, max(0,4) = 4
   *        partition 2:
   *           -> for b: 0, max(0,3) = 3
   *           -> for c: 0, max(0,6) = 6, max(6,8) = 8
   *     2-3)  within partition op :  (u1,u2) => u1+u2
   *          -> shuffle, put values with same key to same partition
   *          -> for a: 3
   *          -> for b: 3
   *          -> for c : 4+8 = 12
   *
   *    ref : https://github.com/yennanliu/KafkaSparkPoc/blob/main/spark/doc/pic/aggregateByKey1.png
   *
   *  3) aggregateByKey definition :
   *
   *   def aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U,
   *       combOp: (U, U) => U): RDD[(K, U)] = self.withScope {
   *       aggregateByKey(zeroValue, defaultPartitioner(self))(seqOp, combOp)
   *      }
   *
   *   // zeroValue : init value for EACH key in EACH partition
   *   // seqOp :  func implements  inside partition, start with init value
   *   // combOp : func implements when aggregate results from ALL partition
   *
   *   4) reduceByKey definition :
   *
   *    def reduceByKey(func: (V, V) => V): RDD[(K, V)] = self.withScope {
   *     reduceByKey(defaultPartitioner(self), func)
   *     }
   *
   *   5) **** aggregateByKey VS reduceByKey
   *    aggregateByKey :
   *      -> cleanedSepOp[V](createZero(), v), SeqOp, combOp, partitioner)
   *      -> has "pre combine" op : SeqOp
   *      -> has init value
   *   reduceByKey :
   *       -> v, func, func, partitioner
   *       -> no init value, use v as init value
   *       -> use func twice, in partition and within partition
   */

  // get max value per key in SAME PARTITION, then sum them up
  // init value per key : 0                         ((zeroValue: U))
  // in partition op : (u,v) => math.max(u,v)       ((U, V) => U)  (seqOp)
  // within partition op :  (u1,u2) => u1+u2        ((U, U) => U) (combOp)
  val r1 = rdd1.aggregateByKey(0)((u,v) => math.max(u,v), (u1,u2) => u1+u2).collect()

  println("r1 = " + r1.toList)

  println("=================")

  /**
   * demo2 :  wordcount via aggregateByKey
   */
  val r2 = rdd1.aggregateByKey(0)((u,v) => u+v, (u1,u2) => u1+u2).collect()
  println("r2 = " + r2.toList)

  val r2_ = rdd1.aggregateByKey(0)(_+_, _+_).collect()  // same as above

  println("=================")

  /**
   * demo3 : wordcount via reduceByKey
   */
  val r3 = rdd1.reduceByKey((x,y) => x+y).collect()
  println("r3 = " + r3.toList)

  val r3_ = rdd1.reduceByKey(_+_).collect() // same as above
}
