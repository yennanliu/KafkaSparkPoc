package com.yen.dev.model

// https://github.com/josephmachado/beginner_de_project_stream/blob/main/src/main/scala/com.startdataengineering/model/ServerLog.scala

case class ServerLog(
                      eventId: String,
                      accountId: Int,
                      eventType: String,
                      locationCountry: String,
                      eventTimeStamp: Long
                    ) extends Serializable {
  override def toString: String = f"$eventId%s,$accountId%s,$eventType%s,$locationCountry%s,$eventTimeStamp%s"
}

object ServerLog {
  def fromString(value: String): ServerLog = {
    val elements: Array[String] = value.split(",")
    ServerLog(elements(0), elements(1).toInt, elements(2), elements(3), elements(4).toLong)
  }
}
