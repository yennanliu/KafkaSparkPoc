package dev

import model.pJson
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.explode

object SparkApp3 {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
      .setAppName("SparkApp3")
      .setMaster("yarn")

    val sc = new SparkContext(conf)

    val spark = SparkSession.builder()
      .appName("SparkApp3")
      .master("yarn")
      .getOrCreate()

    import spark.implicits._

    // define schema for raw json data
    val p_schema = ScalaReflection.schemaFor[pJson].dataType.asInstanceOf[StructType]
    p_schema.printTreeString

    // get df
    val s3_bucket = "s3://xxx/*/*/*/*"
    val dest_s3_path = "s3://xxx/20230529"

    val df_with_schema = spark.read
      .schema(p_schema)
      .option("multiline", true)
      .json(s3_bucket)

    df_with_schema.printSchema()

    println("data count = " + df_with_schema.count())

    df_with_schema.show()

    // flatten df
    val flatten_df = df_with_schema
      .select(explode($"yyy")
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
