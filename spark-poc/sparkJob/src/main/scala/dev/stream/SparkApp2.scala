//package dev.stream
//
//import org.apache.spark
//import org.apache.spark.SparkConf
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions.from_json
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
///**
// * DEV app 2 process Kinesis stream
// *
// * https://www.databricks.com/blog/2017/08/09/apache-sparks-structured-streaming-with-amazon-kinesis-on-databricks.html
// * https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
// */
//
//object SparkApp2 {
//
//  def main(args: Array[String]): Unit = {
//
//
//    val APP_NAME = "Stream SparkApp1"
//    val KINESIS_END_POINT_URL = "kinesis.us-east-1.amazonaws.com"
//    val REGION_NAME = "us-east-1"
//    //val STREAM_NAME = "my_kinesis_stream_2"
//    val KINESIS_APP_NAME = "KINESIS_APP"
//    val KINESIS_STREAM_NAME = "my_kinesis_stream_2"
//    val numStreams = 100
//
////    val conf = new SparkConf()
////      .setAppName("SparkApp2")
////      .setMaster("local[*]")
//    //val ssc = new StreamingContext(conf, Seconds(1))
//
//    val spark = SparkSession.builder()
//      .appName("SparkApp2")
//      .master("local[*]")
//      .getOrCreate()
//
//    import spark.implicits._
//
//    val kinesisDF = spark.readStream
//      .format("kinesis")
//      .option("streamName", KINESIS_STREAM_NAME)
//      .option("initialPosition", "earliest")
//      .option("region", REGION_NAME)
//      .option("awsAccessKey", "")
//      .option("awsSecretKey", "")
//      .load()
//
//    val ToStreamDF = kinesisDF
//      .select("value")
//
//    ToStreamDF.createOrReplaceTempView("to_stream")
//
//    val query = ToStreamDF
//      .writeStream
//      .outputMode("complete")
//      .format("console")
//      .start()
//
//    query.awaitTermination()
//
//
//    // extract data from the payload and use transformation to do
//    // your analytics
////    val dataDevicesDF = kinesisDF
////      .selectExpr("cast (data as STRING) jsonData")
////    )
////    .select(from_json("jsonData", jsonSchema).as("devices"))
////      // explode into its equivalent DataFrame column names
////      .select("devices.*")
////      // filter out some devices with certain attribute values
////      .filter($"devices.temp" > 10 and $"devices.signal" > 15)
//
//  }
//
//
//}
