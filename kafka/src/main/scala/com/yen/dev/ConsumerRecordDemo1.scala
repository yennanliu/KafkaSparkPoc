package com.yen.dev

// https://www.confluent.io/blog/kafka-scala-tutorial-for-beginners/

import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer, OffsetAndTimestamp}
import scala.collection.mutable.ListBuffer


/**
 * // constructor of org.apache.kafka.clients.consumer.ConsumerRecord
 * // genetic type : K,V (key, value)
 * public ConsumerRecord( String topic,
 *                        int partition,
 *                        long offset,
 *                        K key,
 *                        V value) {
 *       this(topic, partition, offset, NO_TIMESTAMP,
 *       TimestampType.NO_TIMESTAMP_TYPE,NULL_CHECKSUM, NULL_SIZE, NULL_SIZE, key, value);
 *    }
 */


object ConsumerRecordDemo1 extends App {

  /** demo 1 : init ConsumerRecord with generic type */
  val my_record1:ConsumerRecord[String, String] = new ConsumerRecord("a",2,100,"key", "value")

  println("my_record1 = " + my_record1)
  println("my_record1.topic = " + my_record1.topic)
  println("my_record1.partition = " + my_record1.partition)
  println("my_record1.offset = " + my_record1.offset)
  println("my_record1.key = " + my_record1.key)
  println("my_record1.value = " + my_record1.value)

  println("=================================")

  /** demo 2 : init ConsumerRecord with different generic type */
  val my_record2:ConsumerRecord[String, Int] = new ConsumerRecord("a",2,100,"key", 999)

  println("my_record2 = " + my_record2)
  println("my_record2.value = " + my_record2.value)

  println("=================================")

  /** demo 3 : map on ConsumerRecord */
  val myEvent1 = ListBuffer(
    new ConsumerRecord("a",2,100,"key", "value"),
    new ConsumerRecord("a",2,100,"key", ListBuffer(1,2,3)),
    new ConsumerRecord("a",2,100,"key", 3)
  )

  myEvent1.foreach(println(_))

  println("=================================")

  val myEvent2 = myEvent1.map{
    x =>
      new ConsumerRecord(x.topic + "_abc", x.partition, x.offset,x.key,x.value)
  }

  myEvent2.foreach(println(_))

}

// some dev case class (not used in this demo)
case class my_event(
                       a:ListBuffer[Int],
                       b:String,
                       // scala shapeless : // https://www.scala-exercises.org/shapeless/generic
                       // https://stackoverflow.com/questions/31346863/scala-class-takes-type-parameters-when-passing-a-class-to-another-class
                       c:ConsumerRecord[String, String]
                     )