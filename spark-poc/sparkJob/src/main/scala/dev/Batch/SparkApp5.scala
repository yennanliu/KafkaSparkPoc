package dev.Batch

import model.{EventLog, RawRecord}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.types.StructType
import org.apache.spark.{SparkConf, SparkContext}

/**
 *  process S3 data output from Lambda, save output to s3
 *
 *  Kinesis -> Lambda -> s3 -> spark (this script) -> s3
 *
 */

object SparkApp5 {

  def main(args: Array[String]): Unit = {

    // s3://firehose-my-kinesis-stream-3/XYZ/firehose-my_kinesis_stream_3_dev1-1-2023-08-03-15-30-16-e5adf553-63b3-3ff6-b05f-2126e05b4c56
    val BUCKET_NAME = "firehose-my-kinesis-stream-3"
    val INPUT_BUCKET_PATH = "eventDate=2023-08-09/"
    val OUTPUT_BUCKET_PATH = "spark_output2/"

    val conf = new SparkConf()
      .setAppName("SparkApp5")
      //.setMaster("local[*]")
      .setMaster("yarn")

    val sc = new SparkContext(conf)

    val spark = SparkSession.builder()
      .appName("SparkApp5")
      //.master("local[*]")
      .master("yarn")
      .getOrCreate()

    import spark.implicits._

    //val path = "/src/main/resources/"
    // "src/main/resources/people.json"
    // val peopleDF = spark.read.json("src/main/resources/people.json")
//    val df = spark.read
//      //.option("multiline", true)
//      .json( "src/main/resources/test.json")


//    df.show()
//
//    df.count()
//
//    df.printSchema()

//    // define schema for raw json data
//    val r_schema = ScalaReflection.schemaFor[RawRecord].dataType.asInstanceOf[StructType]
//    r_schema.printTreeString

    // {"eventType":"type3","id":"3003","machine":"30666b89-e87a-42d7-b760-4c880550ea95","port":22,"env":"dev"}

    // get df
    val s3_bucket = s"s3://${BUCKET_NAME}/${INPUT_BUCKET_PATH}*"
    val dest_s3_path = s"s3://${BUCKET_NAME}/${OUTPUT_BUCKET_PATH}"

    println(">>> s3_bucket = " + s3_bucket)
    println(">>> dest_s3_path = " + dest_s3_path)

//    case class eventLog(
//                         eventType: String,
//                         id: String,
//                         machine: String,
//                         port: Int,
//                         env: String
//                       )

    val _schema = ScalaReflection.schemaFor[EventLog].dataType.asInstanceOf[StructType]

    //val s3_bucket2 = "s3://firehose-my-kinesis-stream-3/eventDate=2023-08-09/*"
    val df_with_schema = spark.read
      .schema(_schema)
      .json(s3_bucket)

    df_with_schema.show()

    //df_with_schema.printSchema()

    println("data count = " + df_with_schema.count())

    df_with_schema.show()

    // flatten df
//    val flatten_df = df_with_schema
//      .select(explode($"hostVulnerabilityList")
//        .as("exploded"))
//      .select("exploded.*")

    val flatten_df = df_with_schema

    flatten_df.show(3)

    // save to s3
    flatten_df
      .write.format("csv")
      .option("header", "true")
      .mode("append").save(dest_s3_path)

  }

}
