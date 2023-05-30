package model

case class sub_class(
                      account_id: String,
                      actionable: String,
                      ami_id: String,
                    )

case class pJson(
                     hostVulnerabilityList: Seq[
                       sub_class
                     ]
                   )
