package model

case class pjson_sub_class(
                      account_id: String,
                      actionable: String,
                      ami_id: String,
                    )

case class PJson(
                     hostVulnerabilityList: Seq[
                       pjson_sub_class
                     ]
                   )
