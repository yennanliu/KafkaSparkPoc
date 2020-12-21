package com.yen.app

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import java.util.{Date, Properties}
import scala.util.Random

object Producer extends App{

  val topic = "raw_data"
  val brokers = "127.0.0.1:9092"

  val props = new Properties()
  props.put("bootstrap.servers", brokers)
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  //  acks could be 0, 1, or -1 (all) : https://docs.confluent.io/current/installation/configuration/producer-configs.html
  //props.put("acks","all")

  val producer = new KafkaProducer[String, String](props)

  // create event
  var n = 0
  while (true){
    n += 1
    val runtime = new Date().getTime().toString
    val msg = s"this is $n event !!!"
    val data = new ProducerRecord[String, String](runtime, msg)
    println(s"*** sending $n data to receiver")
    producer.send(data)
    Thread.sleep(1000) // wait for 1000 millisecond
  }
}
