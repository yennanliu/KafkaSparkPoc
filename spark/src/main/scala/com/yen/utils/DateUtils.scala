package com.yen.utils

import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter, ISODateTimeFormat}

object DateUtils {
  // datetime format
  val yyyyMMFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMM")
  val yyyyMMddFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd")
  val yyyyMMddHHFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMddHH")

  // time zone
  val dateFormatTaipei: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd").withZone(DateTimeZone.forID("Asia/Taipei"))

  // iso time
  val ISO8601Format: DateTimeFormatter = ISODateTimeFormat.dateTime()
}

