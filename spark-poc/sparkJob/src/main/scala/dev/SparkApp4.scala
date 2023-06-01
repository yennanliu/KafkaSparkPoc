package dev

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import java.io.InputStream

object SparkApp4 {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local[*]", "SparkApp4")

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("SparkApp4")
      .getOrCreate()

    import spark.implicits._

    // load file from resource
//    val fileStream: InputStream = scala.io.Source.getClass.getResourceAsStream("/mapping.txt")
//    //println("fileStream = " + fileStream.toString)
//
//    val intputRDD: RDD[String] = sparkSession
//      .sparkContext
//      .makeRDD(scala.io.Source.fromInputStream(fileStream).getLines().toList)
//
//      intputRDD.collect().foreach(println)


    // load df
    // https://spark.apache.org/docs/latest/sql-getting-started.html
    val _mappingDF = spark.read.text("src/main/resources/mapping.txt")
    val mappingDF =  _mappingDF.map(
      f => {
        val elements = f.getString(0).split(",")
        (elements(0), elements(1))
      })
    val peopleDF = spark.read.json("src/main/resources/people.json")

//    mappingDF.show()
//    peopleDF.show()

    // Register the DataFrame as a temporary view
    mappingDF.createOrReplaceTempView("mapping")
    peopleDF.createOrReplaceTempView("people")

    // enrich df with RDD
    val enrichedDF = spark.sql("SELECT p.*, m._2 as brand FROM people p LEFT JOIN mapping m ON m._1 = p.id")
    enrichedDF.show()
  }

}
