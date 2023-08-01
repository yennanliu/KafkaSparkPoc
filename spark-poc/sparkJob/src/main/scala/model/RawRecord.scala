package model

case class rawRecord_subClass(
                     event_type: String,
                     id: String,
                     machine: String,
                     port: Int,
                     env: String
                   )

case class RawRecord(
                      hostVulnerabilityList: Seq[
                        rawRecord_subClass
                      ]
                    )
