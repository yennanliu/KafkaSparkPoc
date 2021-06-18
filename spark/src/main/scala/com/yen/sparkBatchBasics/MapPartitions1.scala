package com.yen.sparkBatchBasics

// https://www.youtube.com/watch?v=TVWJ-YBfKWQ&list=PLmOn9nNkQxJF-qlCCDx9WsdAe6x5hhH77&index=33

import org.apache.spark.SparkContext

object MapPartitions1 extends App {

  /**
   *  MapPartitions
   *
   *
   * // Source code
   *  def mapPartitions[U: ClassTag](
   *         f: Iterator[T] => Iterator[U],
   *         preservesPartitioning: Boolean = false): RDD[U] = withScope {
   *         val cleanedF = sc.clean(f)
   *         new MapPartitionsRDD(
   *         this,
   *         (context: TaskContext, index: Int, iter: Iterator[T]) => cleanedF(iter),
   *         preservesPartitioning)
   *  }
   *
   */

  println("MapPartitions1 start ...")

  val sc = new SparkContext("local[*]", "RddPartition2")

  val rdd1 = sc.parallelize(Array(1,2,3,4))

  val r1 = rdd1.mapPartitions(x => x.map(_ * 2)).collect
  r1.foreach(println(_))

  print("===============")

  /**
   *  "Nearby" principle
   *
   *  Need to define more parameter in mapPartition code is the is a "()"
   *
   *  example 1) :
   *
   *  When we write this : Iterator(_.mkString("|"))
   *  Scala will transform it to y => Iterator( x => x.mkString("|") ) when running  (where y is the input. e.g. rdd1)
   *  but here we DON'T KNOW what x stands for (missing parameter (x) type)
   *  -> So scala will come up exception
   *
   *
   * example 2) :
   *
   *  When we write this :  (_+1) * 2
   *  Scala will transform it to y => (x => (x+1) * 2)  when running    (where y is the input. e.g. rdd1)
   *  but here we DON'T KNOW what x stands for (missing parameter (x) type)
   *  -> So scala will come up exception
   */

  // below not works, since mapPartitions needs an Iterator, while mkString returns a string
  //val r2 = rdd1.mapPartitions(_.mkString(","))

  // below not works as well
  //val r2 = rdd1.mapPartitions(Iterator(_.mkString(",")))

  val r2 = rdd1.mapPartitions(x => Iterator(x.mkString("|"))).collect
  println("r2 = " + r2.toList)


  val r3 = rdd1.map(_*2 + 1).collect()
  print("r3 = " + r3)

  // below one not works, based on the nearby principal
  // scala transforms below to y => (x => (x+1) * 2)    (where y is the input. e.g. rdd1)
  // but we don't know what x is (missing parameter (x) type)
  //val r4 = rdd1.map((_+1) * 2).collect()

  val r4 = rdd1.map( x => (x + 1) * 2 )
  r4.collect()
  println("MapPartitions1 end ...")
}
