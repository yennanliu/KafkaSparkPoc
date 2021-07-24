package com.yen.sparkBatchBasics

// https://sparkbyexamples.com/apache-spark-rdd/spark-rdd-aggregate-example/
// https://www.youtube.com/watch?v=QPTdaEQxE8s&list=PLmOn9nNkQxJF-qlCCDx9WsdAe6x5hhH77&index=51

/**
 *   aggregate RDD
 *
 *   1) syntax
 *     def aggregate[U](zeroValue: U)(seqOp: (U, T) ⇒ U, combOp: (U, U) ⇒ U)
 *        (implicit arg0: ClassTag[U]): U
 *
 *   2) aggregate() is an action
 *
 *   3) usage :
 *     Since RDD’s are partitioned,
 *     the aggregate takes full advantage of it
 *     by first aggregating elements in each partition
 *     and then aggregating results of all partition to get
 *     the final result. and the result could be any type
 *     than the type of your RDD.
 *
 *   4) arguments
 *
 *     4.1) zeroValue – Initial value to be used for each partition in aggregation, this value would be used to initialize the accumulator. we mostly use 0 for integer and Nil for collections.
 *
 *     4.2) seqOp – This operator is used to accumulate the results of each partition, and stores the running accumulated result to U,
 *
 *     4.3) combOp – This operator is used to combine the results of all partitions U.
 *
 */

import org.apache.spark.SparkContext

object aggregate1 extends App {

  val sc = new SparkContext("local[*]", "aggregate1")

  /**
   * example 1
   *
   * -> In below example, param0 is used as seqOp and param1 is used as combOp,
   * On param0 “accu” is an accumulator that accumulates the values for each partition
   * and on param1, accumulates the result of all accululators from all partitions.
   */
  val listRdd = sc.parallelize(List(1,2,3,4,5,3,2))
  def param0= (accu:Int, v:Int) => accu + v
  def param1= (accu1:Int,accu2:Int) => accu1 + accu2
  val result = listRdd.aggregate(0)(param0,param1)

  println("output 1 =>" + result)

  println("==================")

  /**
   * example 2
   *
   * -> in below example,
   *  param3 is used as seqOp
   *  and param4 is used as combOp.
   */
  val inputRDD = sc.parallelize(List(("Z", 1),("A", 20),("B", 30),("C", 40),("B", 30),("B", 60)))
  //aggregate
  def param3= (accu:Int, v:(String,Int)) => accu + v._2
  def param4= (accu1:Int,accu2:Int) => accu1 + accu2
  val result2 = inputRDD.aggregate(0)(param3,param4)

  println("output 2 =>" + result)

}
