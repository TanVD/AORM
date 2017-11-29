package tanvd.aorm

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import ru.yandex.clickhouse.ClickHouseUtil
import sun.reflect.generics.reflectiveObjects.NotImplementedException
import java.math.BigInteger
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.*

sealed class DbType<T> {
    abstract fun toSqlName(): String

    abstract fun getValue(name: String, result: ResultSet): T
    abstract fun getValue(index: Int, result: ResultSet): T
    abstract fun setValue(index: Int, statement: PreparedStatement, value: T)

    abstract fun toStringValue(value: T): String


    /** Equals by sql name **/
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as DbType<*>

        if (toSqlName() != other.toSqlName()) return false

        return true
    }

    override fun hashCode(): Int = toSqlName().hashCode()
}

sealed class DbPrimitiveType<T> : DbType<T>() {
    abstract fun toArray(): DbArrayType<T>
}

sealed class DbArrayType<T> : DbType<List<T>>() {
    abstract fun toPrimitive(): DbPrimitiveType<T>
}

class DbDate : DbPrimitiveType<Date>() {
    companion object {
        val dateFormat = SimpleDateFormat("yyyy-MM-dd")
    }

    override fun toSqlName(): String = "Date"
    override fun getValue(name: String, result: ResultSet): Date = result.getDate(name)
    override fun getValue(index: Int, result: ResultSet): Date = result.getDate(index)
    override fun setValue(index: Int, statement: PreparedStatement, value: Date) {
        statement.setDate(index, java.sql.Date(value.time))
    }

    override fun toStringValue(value: Date): String = "'${dateFormat.format(value)}'"

    override fun toArray(): DbArrayType<Date> = DbArrayDate()
}

class DbDateTime : DbPrimitiveType<DateTime>() {
    companion object {
        val dateTimeFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
    }

    override fun toSqlName(): String = "DateTime"

    override fun getValue(name: String, result: ResultSet): DateTime = DateTime(result.getTimestamp(name).time)
    override fun getValue(index: Int, result: ResultSet): DateTime = DateTime(result.getTimestamp(index).time)

    override fun setValue(index: Int, statement: PreparedStatement, value: DateTime) {
        statement.setTimestamp(index, Timestamp(value.millis))
    }

    override fun toStringValue(value: DateTime): String = "'${dateTimeFormat.print(value)}'"

    override fun toArray(): DbArrayType<DateTime> {
        throw NotImplementedException()
//        return DbArrayDateTime()
    }
}

class DbLong : DbPrimitiveType<Long>() {
    override fun toSqlName(): String = "Int64"

    override fun getValue(name: String, result: ResultSet): Long = result.getLong(name)
    override fun getValue(index: Int, result: ResultSet): Long = result.getLong(index)

    override fun setValue(index: Int, statement: PreparedStatement, value: Long) {
        statement.setLong(index, value)
    }

    override fun toStringValue(value: Long): String = value.toString()

    override fun toArray(): DbArrayType<Long> = DbArrayLong()
}

/**
 * ULong is equal to long in AORM.
 * Do not use it for values, which not fits in long.
 */
class DbULong : DbPrimitiveType<Long>() {
    override fun toSqlName(): String = "UInt64"

    override fun getValue(name: String, result: ResultSet): Long = result.getLong(name)
    override fun getValue(index: Int, result: ResultSet): Long = result.getLong(index)

    override fun setValue(index: Int, statement: PreparedStatement, value: Long) {
        statement.setLong(index, value)
    }

    override fun toStringValue(value: Long): String = value.toString()

    override fun toArray(): DbArrayType<Long> = DbArrayULong()
}

class DbBoolean : DbPrimitiveType<Boolean>() {
    override fun toSqlName(): String = "UInt8"

    override fun getValue(name: String, result: ResultSet): Boolean = result.getInt(name) == 1
    override fun getValue(index: Int, result: ResultSet): Boolean = result.getInt(index) == 1


    override fun setValue(index: Int, statement: PreparedStatement, value: Boolean) {
        if (value) {
            statement.setInt(index, 1)
        } else {
            statement.setInt(index, 0)
        }
    }

    override fun toStringValue(value: Boolean): String = if (value) "1" else "0"

    override fun toArray(): DbArrayType<Boolean> = DbArrayBoolean()
}

class DbString : DbPrimitiveType<String>() {
    override fun toSqlName(): String = "String"

    override fun getValue(name: String, result: ResultSet): String = result.getString(name)
    override fun getValue(index: Int, result: ResultSet): String = result.getString(index)

    override fun setValue(index: Int, statement: PreparedStatement, value: String) {
        statement.setString(index, value)
    }

    override fun toStringValue(value: String): String = "'${ClickHouseUtil.escape(value)}'"

    override fun toArray(): DbArrayType<String> = DbArrayString()
}

class DbArrayDate : DbArrayType<Date>() {
    override fun toSqlName(): String = "Array(Date)"

    @Suppress("UNCHECKED_CAST")
    override fun getValue(name: String, result: ResultSet): List<Date> =
            (result.getArray(name).array as Array<Date>).toList()

    @Suppress("UNCHECKED_CAST")
    override fun getValue(index: Int, result: ResultSet): List<Date> = (result.getArray(index).array as Array<Date>).toList()


    override fun setValue(index: Int, statement: PreparedStatement, value: List<Date>) {
        statement.setArray(index,
                statement.connection.createArrayOf(toSqlName(), value.map {
                    java.sql.Date(it.time)
                }.toTypedArray()))
    }

    override fun toStringValue(value: List<Date>): String =
            value.joinToString(prefix = "[", postfix = "]") { "'${DbDate.dateFormat.format(it)}'" }

    override fun toPrimitive(): DbPrimitiveType<Date> = DbDate()
}

/**
 * Disabled. See https://github.com/yandex/clickhouse-jdbc/issues/153
 */
//class DbArrayDateTime: DbArrayType<DateTime>() {
//    override fun toSqlName(): String {
//        return "Array(DateTime)"
//    }
//
//    override fun getValue(name: String, result: ResultSet): List<DateTime> {
//        return (result.getArray(name).array as Array<Timestamp>).map {
//            DateTime(result.getTimestamp(name).nanos / 1000)
//        }
//    }
//
//    override fun setValue(index: Int, statement: PreparedStatement, value: List<DateTime>) {
//        statement.setArray(index,
//                statement.connection.createArrayOf(toSqlName(), value.map {
//                    Timestamp(it.millis)
//                }.toTypedArray()))
//
//        statement.setArray(index, statement.connection.createArrayOf("Array(DateTime)", array)
//    }
//
//    override fun toStringValue(value: List<DateTime>): String {
//        return value.joinToString(prefix = "[", postfix = "]") { "'${DbDateTime.dateTimeFormat.format(it)}'" }
//    }
//
//    override fun toPrimitive(): DbPrimitiveType<DateTime> {
//        return DbDateTime()
//    }
//
//}

class DbArrayLong : DbArrayType<Long>() {

    override fun toSqlName(): String = "Array(Int64)"

    override fun getValue(name: String, result: ResultSet): List<Long> =
            (result.getArray(name).array as LongArray).toList()

    override fun getValue(index: Int, result: ResultSet): List<Long> = (result.getArray(index).array as LongArray).toList()


    override fun setValue(index: Int, statement: PreparedStatement, value: List<Long>) {
        statement.setArray(index,
                statement.connection.createArrayOf(toSqlName(), value.toTypedArray()))
    }

    override fun toStringValue(value: List<Long>): String =
            value.joinToString(prefix = "[", postfix = "]") { it.toString() }

    override fun toPrimitive(): DbPrimitiveType<Long> = DbLong()
}

class DbArrayULong : DbArrayType<Long>() {
    override fun toSqlName(): String = "Array(UInt64)"

    @Suppress("UNCHECKED_CAST")
    override fun getValue(name: String, result: ResultSet): List<Long> =
            (result.getArray(name).array as Array<BigInteger>).map { it.toLong() }.toList()

    @Suppress("UNCHECKED_CAST")
    override fun getValue(index: Int, result: ResultSet): List<Long> =
            (result.getArray(index).array as Array<BigInteger>).map { it.toLong() }.toList()


    override fun setValue(index: Int, statement: PreparedStatement, value: List<Long>) {
        statement.setArray(index,
                statement.connection.createArrayOf(toSqlName(), value.toTypedArray()))
    }

    override fun toStringValue(value: List<Long>): String =
            value.joinToString(prefix = "[", postfix = "]") { it.toString() }

    override fun toPrimitive(): DbPrimitiveType<Long> = DbULong()
}

class DbArrayBoolean : DbArrayType<Boolean>() {
    override fun toSqlName(): String = "Array(UInt8)"

    override fun getValue(name: String, result: ResultSet): List<Boolean> {
        return (result.getArray(name).array as LongArray).map {
            it.toInt() == 1
        }
    }

    override fun getValue(index: Int, result: ResultSet): List<Boolean> {
        return (result.getArray(index).array as LongArray).map {
            it.toInt() == 1
        }
    }

    override fun setValue(index: Int, statement: PreparedStatement, value: List<Boolean>) {
        statement.setArray(index,
                statement.connection.createArrayOf(toSqlName(), value.map { if (it) 1 else 0 }.toTypedArray()))
    }

    override fun toStringValue(value: List<Boolean>): String =
            value.joinToString(prefix = "[", postfix = "]") { if (it) "1" else "0" }

    override fun toPrimitive(): DbPrimitiveType<Boolean> = DbBoolean()
}

class DbArrayString : DbArrayType<String>() {
    override fun toSqlName(): String = "Array(String)"

    @Suppress("UNCHECKED_CAST")
    override fun getValue(name: String, result: ResultSet): List<String> =
            (result.getArray(name).array as Array<String>).toList()

    @Suppress("UNCHECKED_CAST")
    override fun getValue(index: Int, result: ResultSet): List<String> =
            (result.getArray(index).array as Array<String>).toList()


    override fun setValue(index: Int, statement: PreparedStatement, value: List<String>) {
        statement.setArray(index,
                statement.connection.createArrayOf(toSqlName(), value.toTypedArray()))
    }

    override fun toStringValue(value: List<String>): String =
            value.joinToString(prefix = "[", postfix = "]") { "'${ClickHouseUtil.escape(it)}'" }

    override fun toPrimitive(): DbPrimitiveType<String> = DbString()
}
