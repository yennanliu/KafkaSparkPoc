package util

//import org.joda.time.format.{DateTimeFormat, DateTimeFormatter, ISODateTimeFormat}
//import org.joda.time.{DateTime, DateTimeZone, Hours, Years}
import org.slf4j.LoggerFactory

import java.time.{LocalTime, ZoneId}
import java.time.format.DateTimeFormatter

object DateTimeUtils {

  val yearMonthDateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  val yearMonthDateHourFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH")

  // TODO : fix this
  def currentYearMonthDateUTC: DateTimeFormatter = {
    yearMonthDateFormat.withZone(ZoneId.of("UTC"))
  }

}
