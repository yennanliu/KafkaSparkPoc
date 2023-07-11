package util

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.explode

object DFUtil {

  def flattenDF(dataFrame :DataFrame, explodeColumn : Column): DataFrame = {

    val flattenedDF = dataFrame
      .select(explode(${explodeColumn}).as("exploded"))
      .select("exploded.*")

    flattenedDF
  }

}
