package model

case class EventLog(
                     timeStamp: Int,
                     eventType: String,
                     id: String,
                     machine: String,
                     port: Int,
                     env: String
                   )
