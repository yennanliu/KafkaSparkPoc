package util

import org.apache.spark
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.types.StructType

object DFUtil {

//  def flattenDF(dataFrame :DataFrame, explodeColumn : Column): DataFrame = {
//
//    val flattenedDF = dataFrame
//      .select(explode(${explodeColumn}).as("exploded"))
//      .select("exploded.*")
//
//    flattenedDF
//  }

  def loadDFWithSchema(spark:SparkSession, schema:StructType, s3BucketPath: String): DataFrame = {
    spark.read
      .schema(schema)
      .option("multiline", true)
      .json(s3BucketPath)
  }

}
