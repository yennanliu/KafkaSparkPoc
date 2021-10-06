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


object eventTransform1 extends App {

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

}

case class A(a:String)
case class B(b:Int)

case class my_event(
                       a:ListBuffer[Int],
                       b:String,
                       // scala shapeless : // https://www.scala-exercises.org/shapeless/generic
                       // https://stackoverflow.com/questions/31346863/scala-class-takes-type-parameters-when-passing-a-class-to-another-class
                       c:ConsumerRecord[String, String]
                     )