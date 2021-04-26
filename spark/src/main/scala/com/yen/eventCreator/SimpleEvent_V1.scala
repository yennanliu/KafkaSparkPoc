package com.yen.eventCreator

import java.io.PrintWriter
import java.net.ServerSocket
import java.util.Random
import java.time.LocalDateTime

import com.google.gson.Gson

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

  case class simpleEvent(id: Int, event_date: Long, msg: String)

  while (true) {
    val socket = listener.accept()
    new Thread() {
      override def run(): Unit = {
        println(s"Got client connected from: ${socket.getInetAddress}")
        val out = new PrintWriter(socket.getOutputStream(), true)

        while (true) {

          val r = scala.util.Random
          val id = r.nextInt(10000000)
          val event_date = System.currentTimeMillis

          val someRandom = r.nextInt(10)

          val msg:String = someRandom match {
            case someRandom if someRandom % 2 == 0 => "hello yo !!!"
            case someRandom if someRandom % 2 == 1 => "good day ~"
            case _ => "wazzup ?"
          }

          val payload = new simpleEvent(id, event_date, msg)

          Thread.sleep(sleepDelayMs)
          val msgDataJson = new Gson().toJson(payload)
          println("*** msgDataJson = " + msgDataJson.toString)
          out.write(msgDataJson)
          out.flush()
        }
        socket.close()
      }
    }.start()
  }
}
