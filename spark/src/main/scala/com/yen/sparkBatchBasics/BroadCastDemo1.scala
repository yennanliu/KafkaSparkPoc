package com.yen.sparkBatchBasics

// https://spark.apache.org/docs/3.0.2/api/java/org/apache/spark/broadcast/Broadcast.html
// https://www.educba.com/spark-broadcast/
// https://www.waitingforcode.com/apache-spark/serialization-issues-part-1/read
// https://www.waitingforcode.com/apache-spark/serialization-issues-part-2/read

import org.apache.spark.SparkContext

object BroadCastDemo1 extends App {

  val sc = new SparkContext("local[*]", "BroadCastDemo1")

  // broadcast
  val test_class1 = new myTestClass1()
  //sc.broadcast(test_class1) // Exception in thread "main" java.io.NotSerializableException: com.yen.sparkBatchBasics.myTestClass1

  val test_class2 = new myTestClass2()
  val _test_class2 = sc.broadcast(test_class2) // this one is OK, since myTestClass2 already implemented Serializable

  val rdd1 = sc.parallelize(Array((1,"dd"),(2,"cc"),(3,"ss")))

  println("***********")
  val _test_class2_from_broadcast = _test_class2.value
  println(_test_class2_from_broadcast.shout("should zzzzzzzz"))
  println(_test_class2.value.shout("shout !!!!!"))
  println("***********")

  println(rdd1.collect().toList)
}

class myTestClass1 extends baseClass {
  def shout(x:String):Unit = {
    println("x !!!!")
  }
}

class myTestClass2 extends baseClass with Serializable {
  def shout(x:String):Unit = {
    println(s"$x ~~~~~~~")
  }
}

abstract class baseClass {

}
