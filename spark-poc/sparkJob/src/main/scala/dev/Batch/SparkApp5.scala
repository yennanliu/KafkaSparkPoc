package dev.Batch

import model.EventLog
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.{col, explode, from_unixtime}
import org.apache.spark.sql.types.StructType
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.util.Calendar

/**
 *  process S3 data output from Lambda, save output to s3
 *
 *  Kinesis -> Lambda -> s3 -> spark (this script) -> s3
 *
 */

object SparkApp5 {


  def main(args: Array[String]): Unit = {

    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    val _today = formatter.format(Calendar.getInstance().getTime())

    val BUCKET_NAME = "firehose-my-kinesis-stream-3"
    val INPUT_BUCKET_PATH = s"eventDate=${_today}/"
    val OUTPUT_BUCKET_PATH = s"spark_raw_output/insert_date=${_today}"
    val AGGRE_OUTPUT_BUCKET_PATH = s"spark_aggr_output/insert_date=${_today}"

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
//    df.count()
//    df.printSchema()

    // get df
    val s3_bucket = s"s3://${BUCKET_NAME}/${INPUT_BUCKET_PATH}*"
    val dest_s3_path = s"s3://${BUCKET_NAME}/${OUTPUT_BUCKET_PATH}"
    val aggre_dest_s3_path = s"s3://${BUCKET_NAME}/${AGGRE_OUTPUT_BUCKET_PATH}"

    println(">>> s3_bucket = " + s3_bucket)
    println(">>> dest_s3_path = " + dest_s3_path)

    val _schema = ScalaReflection.schemaFor[EventLog].dataType.asInstanceOf[StructType]

    //val s3_bucket2 = "s3://firehose-my-kinesis-stream-3/eventDate=2023-08-09/*"
    val df_with_schema = spark.read
      .schema(_schema)
      .json(s3_bucket)

    df_with_schema.show()

    //df_with_schema.printSchema()

    println("data count = " + df_with_schema.count())

    df_with_schema.show()

    df_with_schema.show(3)

    // save raw to s3
    df_with_schema
      .write.format("csv")
      .option("header", "true")
      .mode("append").save(dest_s3_path)

    // save aggr to s3
    val df_aggr = df_with_schema.select(
      col("machine").as("machine"),
      col("eventType").as("eventType"),
      col("timeStamp").as("timeStamp")
      //from_unixtime(col("timeStamp"),"MM-dd-yyyy HH:mm:ss").as("_timestamp"),
       // from_unixtime(col("timeStamp"),"MM-dd-yyyy HH:mm:ss").as("_timestamp")
    ).groupBy("timeStamp", "machine").count()

    df_aggr
      .write.format("csv")
      .option("header", "true")
      .mode("append").save(aggre_dest_s3_path)
  }

}
