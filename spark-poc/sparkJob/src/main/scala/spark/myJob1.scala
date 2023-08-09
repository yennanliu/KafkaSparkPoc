package spark

import driver.SparkDriver
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object myJob1 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("myJob1")
      .setMaster("local[*]")

    //val s_driver = SparkDriver.getSparkSession("myJob1");
    val s_context = SparkDriver.getSparkContext("myJob1", conf);
    val spark = SparkDriver

    //println("s_driver = " + s_driver)
    println("s_context = " + s_context)
    println("spark = " + spark)

    val rdd1 = s_context.makeRDD(1 to 4, 2)

    println(rdd1.aggregate(0)(_ + _, _ + _)) // 10

    println(rdd1.aggregate(1)(_ + _, _ + _)) // 13
  }

}
