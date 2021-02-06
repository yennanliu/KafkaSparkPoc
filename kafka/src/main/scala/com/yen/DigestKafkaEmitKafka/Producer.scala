package com.yen.DigestKafkaEmitKafka

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import java.util.{Date, Properties}
import scala.util.Random

object Producer extends App {

  val topic = "event_raw"
  val brokers = "127.0.0.1:9092"

  val props = new Properties()
  props.put("bootstrap.servers", brokers)
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  // create producer
  val producer = new KafkaProducer[String, String](props)

  // a simple func that can return different event by cases
  def getEvent(randomSeed: Int):String = {
    randomSeed match {
      case 0 => "???"
      case 1 => "what ??? ???"
      case 2 => "this is event !!!"
      case 3 => "123456"
      case 4 => "abc"
    }
  }

  // let's create some endless events
  var n = 0
  while (true){
    n += 1
    val runtime = new Date().getTime().toString
    val r = new scala.util.Random
    val randomInt = r.nextInt(5)
    val msg = s"this is $n event | " + getEvent(randomInt)
    // need to put "topic" as 1st argument
    val data = new ProducerRecord[String, String](topic, s"runtime : $runtime , msg : $msg")
    println(s"*** sending $n data to receiver | data : $data")
    producer.send(data)
    Thread.sleep(1000) // wait for 1000 millisecond
  }
}
