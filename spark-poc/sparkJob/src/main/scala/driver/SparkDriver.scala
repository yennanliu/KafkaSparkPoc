package driver

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object SparkDriver {

  val appName: String = "defaultSparkApp"
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    // attr
    val arguments: String = "someArgument"
    logger.debug("get spark session")
    val spark:SparkSession = getSparkSession(appName)
    logger.debug(f"spark session = ${spark}")

    def getSparkSession(appName: String, executionType: String = "cluster", cores: Int = 3): SparkSession = {
      // TODO : setup for local, cluster... mode
      SparkSession
        .builder()
        .appName(appName)
        .enableHiveSupport()
        .getOrCreate()
    }
  }

}
