package com.yen.Producer

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import java.util.{Date, Properties}
import scala.util.Random

object producerV1 extends App {

  val topic = "raw_data"
  val brokers = "127.0.0.1:9092"

  val props = new Properties()
  props.put("bootstrap.servers", brokers)
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  case class msg(id:Int, time:String, msg:String)

  val id = Random.nextInt(1000)
  val runtime = new Date().getTime().toString
  var n = 0

  while (true){
    n += 1
    msg(id, runtime, "this is msg !!!")
    //println(s"*** msg = $msg")

    // need to put "topic" as 1st argument
    val data = new ProducerRecord[String, String](topic, s"runtime : $runtime , msg : $msg")
    println(s"*** sending $n data to receiver | data : $data")
    producer.send(data)
    Thread.sleep(1000) // wait for 1000 millisecond
  }
}
