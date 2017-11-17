package utils

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import java.text.SimpleDateFormat
import java.util.*

/** Pattern yyyy-MM-dd. **/
fun getDate(date: String): Date = SimpleDateFormat("yyyy-MM-dd").parse(date)

/** Pattern yyyy-MM-dd HH:mm:ss. **/
fun getDateTime(dateTime: String): DateTime = DateTime.parse(dateTime, DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"))