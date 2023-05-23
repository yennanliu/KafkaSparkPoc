package com.yen.poc1

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.explode
import scala.reflect.internal.util.TableDef.Column

object test1 extends App {

  val spark = SparkSession
    .builder
    .appName(this.getClass.getName)
    .master("local[*]")
    .getOrCreate()

  // load from s3
  val s3_bucket = "s3://xxx/yyy"
  val nested: sql.DataFrame = spark.read.option("multiline", true).json(s3_bucket)
  // TODO: fix type ?
  val flatten_df = nested
    .select(explode($"hostVulnerabilityList").as("exploded"))
    .select("exploded.*")

  flatten_df.printSchema

  // set up config
  val jdbc_iam_url = "jdbc:redshift:iam://redshift-spark-redshift-cluster.zzzz.us-east-1.redshift.amazonaws.com:5439/dev"
  val temp_dir = "s3://redshift-spark-databucket-zzzz/tmp/"
  val aws_role = "arn:aws:iam::123:role/redshift-spark-RedshiftIamRole-123"

  // Set query group for the query
  val queryGroup = "spark-redshift-emr"
  val jdbc_iam_url_withQueryGroup = jdbc_iam_url + "?queryGroup=" + queryGroup

  // Set User name for the query
  val userName = "redshiftmasteruser"
  val jdbc_iam_url_withUserName = jdbc_iam_url + "?user=" + userName


  val redshiftOptions = Map(
    "url" -> jdbc_iam_url_withUserName,
    "tempdir" -> temp_dir,
    "aws_iam_role" -> aws_role
  )


  // write to redshift
  // save to redshift
  flatten_df.write
    .format("io.github.spark_redshift_community.spark.redshift")
    .option("url", jdbc_iam_url_withUserName)
    .option("dbtable", "nested")
    .option("tempdir", temp_dir)
    .option("aws_iam_role", aws_role)
    .mode("error")
    .save()

  // check
  val flatten_df_dump = (
    spark.read
      .format("io.github.spark_redshift_community.spark.redshift")
      .options(redshiftOptions)
      .option("dbtable", "nested")
      .load()
    )

  flatten_df_dump.show()

}
