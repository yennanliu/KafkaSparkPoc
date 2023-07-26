package dev.Batch

import org.apache.spark.{SparkConf, SparkContext}

// https://ganguly-04.medium.com/commissioning-emr-spark-cluster-in-aws-and-accessing-it-via-an-edge-node-63a5f1e47d8b

object SparkApp2 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("SparkApp2")
      .setMaster("yarn")

    //val sc = new SparkContext("local[*]", "SparkApp2")

    val sc = new SparkContext(conf)

    // demo 1
    val rdd1 = sc.parallelize(Array(1, 2, 3, 4))

    val sample1 = rdd1.sample(true, 0.4, 2).collect()

    println("sample1 = " + sample1.toList)

    // demo 2
    val rdd2 = sc.parallelize(1 to 30)

    val sample2 = rdd2.sample(false, 0.3, 3).collect()

    println("sample2 = " + sample2.toList)
  }

}
