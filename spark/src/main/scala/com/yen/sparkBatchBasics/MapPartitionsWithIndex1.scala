package com.yen.sparkBatchBasics

// https://www.youtube.com/watch?v=Nx3JhIN-id0&list=PLmOn9nNkQxJF-qlCCDx9WsdAe6x5hhH77&index=34

/**
 *
 *
 *  /**
 *  * Return a new RDD by applying a function to each partition of this RDD, while tracking the index
 *  * of the original partition.
 *  * `preservesPartitioning` indicates whether the input function preserves the partitioner, which
 *  * should be `false` unless this is a pair RDD and the input function doesn't modify the keys.
 *  */
 * def mapPartitionsWithIndex[U: ClassTag](
 *        f: (Int, Iterator[T]) => Iterator[U],
 *        preservesPartitioning: Boolean = false): RDD[U] = withScope {
 *        val cleanedF = sc.clean(f)
 *       new MapPartitionsRDD(
 *       this,
 *       (context: TaskContext, index: Int, iter: Iterator[T]) => cleanedF(index, iter),
 *       preservesPartitioning)
 *        }
 *
 */


import org.apache.spark.SparkContext

object MapPartitionsWithIndex1 extends App {

  println("MapPartitionsWithIndex1 start ...")

  val sc = new SparkContext("local[*]", "MapPartitionsWithIndex1")

  val rdd1 = sc.parallelize(Array(1,2,3,4), 2) // force split RDD to 2 partition

  /** demo 1 : MapPartitionsWithIndex */
  val r1 = rdd1.mapPartitionsWithIndex((i, items) => items.map(x => (i, x))).collect()

  println(r1.toList) //List((0,1), (0,2), (1,3), (1,4))


  val rdd2 = sc.parallelize(Array(1,2,3,4,5,5,6,7,8)) // if not define partition, spark will use default one (# of cpu)
  val r2 = rdd2.mapPartitionsWithIndex((i, items) => items.map(x => (i, x))).collect()

  println(r2.toList) // List((0,1), (1,2), (2,3), (3,4), (4,5), (5,5), (6,6), (7,7), (7,8))

  println("MapPartitionsWithIndex1 end ...")
}
