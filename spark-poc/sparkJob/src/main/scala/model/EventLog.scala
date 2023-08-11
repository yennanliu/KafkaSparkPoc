package model

case class EventLog(
                     timeStamp: Long,
                     eventType: String,
                     id: String,
                     machine: String,
                     port: Int,
                     env: String
                   )
