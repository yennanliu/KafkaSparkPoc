package spark

import config.AppConfig
import driver.SparkDriver
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object myBatchJob1 {

  def main(args: Array[String]): Unit = {

    val logger = LoggerFactory.getLogger(this.getClass)
    //logger.info(AppConfig.dump)

    val conf = new SparkConf()
      .setAppName("myBatchJob1")
      .setMaster("local[*]")

    val sc = SparkDriver.getSparkContext("myBatchJob1", conf)
    val spark = SparkDriver
    println("sc = " + sc)
    println("spark = " + spark)

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
