package dev.Batch

import model.RawRecord
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

    val BUCKET_NAME = "kinesis-lambda-bucket-test-1"
    val INPUT_BUCKET_PATH = "my_kinesis_stream_3/"
    val OUTPUT_BUCKET_PATH = "my_kinesis_stream_3_output/"

    val conf = new SparkConf()
      .setAppName("SparkApp5")
      .setMaster("yarn")

    val sc = new SparkContext(conf)

    val spark = SparkSession.builder()
      .appName("SparkApp5")
      .master("yarn")
      .getOrCreate()

    import spark.implicits._

    // define schema for raw json data
    val r_schema = ScalaReflection.schemaFor[RawRecord].dataType.asInstanceOf[StructType]
    r_schema.printTreeString

    // get df
    val s3_bucket = s"s3://${BUCKET_NAME}/${INPUT_BUCKET_PATH}"
    val dest_s3_path = s"s3://${BUCKET_NAME}/${OUTPUT_BUCKET_PATH}"

    println(">>> s3_bucket = " + s3_bucket)
    println(">>> dest_s3_path = " + dest_s3_path)

    val df_with_schema = spark.read
      .schema(r_schema)
      .option("multiline", true)
      .json(s3_bucket)

    //df_with_schema.printSchema()

    println("data count = " + df_with_schema.count())

    df_with_schema.show()

    // flatten df
    val flatten_df = df_with_schema
      .select(explode($"hostVulnerabilityList")
        .as("exploded"))
      .select("exploded.*")

    flatten_df.show(3)

    // save to s3
    flatten_df
      .write.format("csv")
      .option("header", "true")
      .mode("append").save(dest_s3_path)

  }

}
