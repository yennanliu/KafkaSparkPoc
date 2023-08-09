package model

case class EventLog(
                     eventType: String,
                     id: String,
                     machine: String,
                     port: Int,
                     env: String
                   )
