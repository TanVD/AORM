package tanvd.aorm.utils

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import java.sql.Date

/** Pattern yyyy-MM-dd. **/
fun getDate(date: String): Date = Date.valueOf(date)

/** Pattern yyyy-MM-dd HH:mm:ss. **/
fun getDateTime(dateTime: String): DateTime = DateTime.parse(dateTime, DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"))