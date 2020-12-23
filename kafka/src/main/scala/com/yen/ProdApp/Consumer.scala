package com.yen.ProdApp

import java.util.{Collections, Properties}
import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConverters._

object Consumer extends App{

  val topic = "raw_data"
  val brokers = "localhost:9092"
  val group_id = "group1"

  val props:Properties = new Properties()
  props.put("group.id", group_id)
  props.put("bootstrap.servers", brokers)
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
  props.put("enable.auto.commit", "true")
  props.put("auto.commit.interval.ms", "1000")

  val consumer = new KafkaConsumer(props)

  // TODO : check if this is necessary
  val topics = List(topic)

  // collect event
  try{
    consumer.subscribe(topics.asJava)
    while (true){
      val records = consumer.poll(10) // wait 10 milliseconds for time out ?
      for (record <- records.asScala){
        println(
          s"""
            | topic = ${record.topic()}
            | key = ${record.key()}
            | value = ${record.value()}
            | offset = ${record.offset()}
            | partition = ${record.partition()}
            |""".stripMargin
        )
      }
    }

  } catch {
    case e:Exception => e.printStackTrace()
      println(s"Exception : ${e.printStackTrace()}")
  } finally {
    consumer.close()
  }
}
