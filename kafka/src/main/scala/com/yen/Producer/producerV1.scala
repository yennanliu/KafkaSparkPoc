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

  def make_msg_body(n: Integer): String = n match {
    case _ if n % 3 == 0 => "hello worlddddddd"
    case _ if n % 3 == 1 => "???!!! ..???  !! @@ ??"
    case _ if n % 3 == 2 => "wazz up !!!!!!!"
    case _ => "hi there ~"
  }

  var n = 0

  while (true){

    val id = Random.nextInt(1000)
    val runtime = new Date().getTime().toString

    n += 1

    val event_msg = msg(id, runtime, make_msg_body(id))
    //println(s"*** msg = $msg")

    // need to put "topic" as 1st argument
    val data = new ProducerRecord[String, String](topic, s"runtime : $runtime , msg : $event_msg")
    println(s"*** sending $n data : $data")
    producer.send(data)
    Thread.sleep(1000) // wait for 1000 millisecond
  }
}
