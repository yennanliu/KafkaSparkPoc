package com.yen.eventCreator

import java.io.PrintWriter
import java.net.ServerSocket
import java.util.Random
import java.time.{LocalDateTime, LocalTime}

import com.google.gson.Gson
import spray.json.DefaultJsonProtocol
import spray.json.DefaultJsonProtocol._


/**
 *  SimpleEvent_V1
 *
 *  1) create simple events (simulate kafka) sending to port 9999
 *  2) data type : json
 *  3) ref : https://github.com/yennanliu/NYC_Taxi_Pipeline/blob/master/src/main/scala/TaxiEvent/CreateBasicTaxiEvent.scala
 */

// plz open the other terminal an run below command as socket first
// nc -lk 9999
// nc -lk 9999 > event.txt 2>&1 (? to fix)

object SimpleEvent_V1 extends App {

  val port = 9999
  val viewsPerSecond = 10
  val sleepDelayMs = (1000.0 / viewsPerSecond).toInt
  val listener = new ServerSocket(port)
  println(s"Listening on port: $port")

  /** case class for event, and transform to json */
  // https://index.scala-lang.org/spray/spray-json/spray-json/1.2.5?target=_2.10
  case class simpleEvent(id: Int, event_date: String, msg: String)
  object simpleEvent

  object MyJsonProtocol extends DefaultJsonProtocol {
    implicit val colorFormat = jsonFormat3(simpleEvent.apply)
  }

  while (true) {
    val socket = listener.accept()
    new Thread() {
      override def run(): Unit = {
        println(s"Got client connected from: ${socket.getInetAddress}")
        val out = new PrintWriter(socket.getOutputStream(), true)

        while (true) {

          val r = scala.util.Random
          val id = r.nextInt(10000000)
          //val event_date = System.currentTimeMillis
          val event_date = LocalDateTime.now()

          val someRandom = r.nextInt(10)

          val msg:String = someRandom match {
            case someRandom if someRandom % 2 == 0 => "hello yo !!!"
            case someRandom if someRandom % 2 == 1 => "good day ~"
            case _ => "wazzup ?"
          }

          import MyJsonProtocol._
          import spray.json._

          //val payload = new simpleEvent(id, event_date.toString, msg)

          val json = new simpleEvent(id, event_date.toString, msg).toJson
          val simpleEvent_ = json.convertTo[simpleEvent]

          Thread.sleep(sleepDelayMs)
          println("*** msgDataJson = " + simpleEvent_.toString)
          out.write(simpleEvent_.toString)
          out.flush()
        }
        socket.close()
      }
    }.start()
  }
}
