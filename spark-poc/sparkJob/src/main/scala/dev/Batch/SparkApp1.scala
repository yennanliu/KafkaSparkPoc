package dev.Batch

// https://medium.com/@Sushil_Kumar/setting-up-spark-with-scala-development-environment-using-intellij-idea-b22644f73ef1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object SparkApp1 {

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.INFO)

    println(">>> SparkApp1 start ")

    val sc = new SparkContext("local[*]", "SparkApp1")

    //println("spark home = " + sc.getConf.get("spark.home").toString)

    /**
     * def aggregate[U : ClassTag](zeroValue: U)(seqOp: (U, T) => U,combOp: (U, U) => U)
     *
     * so
     * in rdd1.aggregate(1)(_+_, _+_) case,
     * zeroValue will +1
     * seqOp will + 1
     */

    val rdd1 = sc.makeRDD(1 to 4, 2)

    println(rdd1.aggregate(0)(_ + _, _ + _)) // 10

    println(rdd1.aggregate(1)(_ + _, _ + _)) // 13

    println("================")

    val rdd2 = sc.makeRDD(1 to 4, 4)

    println(rdd2.aggregate(1)(_ + _, _ + _)) // 15

    println("================")

    // compare : fold

    println(rdd1.fold(0)(_ + _)) // 10

    println(rdd1.fold(1)(_ + _)) // 13

    println(">>> SparkApp1 end ")
  }

}
