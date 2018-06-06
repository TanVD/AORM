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
import kotlin.collections.LinkedHashMap
import kotlin.reflect.KClass

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

//Enums

abstract class DbEnum<T: Enum<*>>(val enumMapping: LinkedHashMap<String, Int>, val enumType: KClass<T>) : DbPrimitiveType<T>() {


    protected fun mappingToSql() = enumMapping.entries.joinToString { "\'${it.key}\' = ${it.value}" }

    override fun getValue(name: String, result: ResultSet): T = enumType.java.enumConstants.first { it.name == result.getString(name) }

    override fun getValue(index: Int, result: ResultSet): T = enumType.java.enumConstants.first { it.name == result.getString(index) }

    override fun setValue(index: Int, statement: PreparedStatement, value: T) {
        statement.setString(index, value.name)
    }

    override fun toStringValue(value: T): String = value.name

    override fun toArray(): DbArrayType<T> {
        throw NotImplementedException()
    }
}

class DbEnum8<T: Enum<*>>(enumMapping: LinkedHashMap<String, Int>, enumType: KClass<T>) : DbEnum<T>(enumMapping, enumType) {
    constructor(enumType: KClass<T>): this(LinkedHashMap<String, Int>().also { map ->
        enumType.java.enumConstants.forEach {
            map[it.name] = it.ordinal
        }
    }, enumType)

    override fun toSqlName(): String = "Enum8(${mappingToSql()})"
}

class DbEnum16<T: Enum<*>>(enumMapping: LinkedHashMap<String, Int>, enumType: KClass<T>) : DbEnum<T>(enumMapping, enumType) {
    constructor(enumType: KClass<T>): this(LinkedHashMap<String, Int>().also { map ->
        enumType.java.enumConstants.forEach {
            map[it.name] = it.ordinal
        }
    }, enumType)

    override fun toSqlName(): String = "Enum16(${mappingToSql()})"
}

//Date and DateTime

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

//Int primitive types

abstract class DbIntPrimitiveType<T: Number>(val sqlName: String,
                                    val getByNameFunc: (ResultSet, String) -> T,
                                    val getByIndexFunc: (ResultSet, Int) -> T,
                                    val setByIndexFunc: (PreparedStatement, Int, T) -> Unit,
                                    val getArrayType: () -> DbArrayType<T>): DbPrimitiveType<T>() {
    override fun toSqlName(): String = sqlName

    override fun getValue(name: String, result: ResultSet) = getByNameFunc(result, name)
    override fun getValue(index: Int, result: ResultSet) = getByIndexFunc(result, index)

    override fun setValue(index: Int, statement: PreparedStatement, value: T) {
        setByIndexFunc(statement, index, value)
    }

    override fun toStringValue(value: T): String = value.toString()

    override fun toArray() = getArrayType()

}

class DbInt8 : DbIntPrimitiveType<Byte>("Int8", {resultSet, name -> resultSet.getByte(name)}, {resultSet, index -> resultSet.getByte(index)},
        {statement, index, value -> statement.setByte(index, value) }, { DbArrayInt8() })

class DbUInt8 : DbIntPrimitiveType<Byte>("UInt8", {resultSet, name -> resultSet.getByte(name)}, {resultSet, index -> resultSet.getByte(index)},
        {statement, index, value -> statement.setByte(index, value) }, { DbArrayUInt8() })

class DbInt16 : DbIntPrimitiveType<Short>("Int16", {resultSet, name -> resultSet.getShort(name)}, {resultSet, index -> resultSet.getShort(index)},
        {statement, index, value -> statement.setShort(index, value) }, { DbArrayInt16() })

class DbUInt16 : DbIntPrimitiveType<Short>("UInt16", {resultSet, name -> resultSet.getShort(name)}, {resultSet, index -> resultSet.getShort(index)},
        {statement, index, value -> statement.setShort(index, value) }, { DbArrayUInt16() })

class DbInt32 : DbIntPrimitiveType<Int>("Int32", {resultSet, name -> resultSet.getInt(name)}, {resultSet, index -> resultSet.getInt(index)},
        {statement, index, value -> statement.setInt(index, value) }, { DbArrayInt32() })

class DbUInt32 : DbIntPrimitiveType<Int>("UInt32", {resultSet, name -> resultSet.getInt(name)}, {resultSet, index -> resultSet.getInt(index)},
        {statement, index, value -> statement.setInt(index, value) }, { DbArrayUInt32() })

class DbInt64 : DbIntPrimitiveType<Long>("Int64", {resultSet, name -> resultSet.getLong(name)}, {resultSet, index -> resultSet.getLong(index)},
        {statement, index, value -> statement.setLong(index, value) }, { DbArrayInt64() })

class DbUInt64 : DbIntPrimitiveType<Long>("UInt64", {resultSet, name -> resultSet.getLong(name)}, {resultSet, index -> resultSet.getLong(index)},
        {statement, index, value -> statement.setLong(index, value) }, { DbArrayUInt64() })

//Int array types

abstract class DbIntArrayType<T: Number>(val sqlName: String,
                                         val getByNameFunc: (ResultSet, String) -> List<T>,
                                         val getByIndexFunc: (ResultSet, Int) -> List<T>,
                                         val setByIndexFunc: (PreparedStatement, Int, List<T>) -> Unit,
                                         val getPrimitiveType: () -> DbPrimitiveType<T>): DbArrayType<T>() {
    override fun toSqlName(): String = sqlName

    override fun getValue(name: String, result: ResultSet) = getByNameFunc(result, name)
    override fun getValue(index: Int, result: ResultSet) = getByIndexFunc(result, index)

    override fun setValue(index: Int, statement: PreparedStatement, value: List<T>) {
        setByIndexFunc(statement, index, value)
    }

    override fun toStringValue(value: List<T>): String = value.joinToString(prefix = "[", postfix = "]") { it.toString() }

    override fun toPrimitive(): DbPrimitiveType<T> = getPrimitiveType()
}

class DbArrayInt8 : DbIntArrayType<Byte>("Array(Int8)",
        {resultSet, name -> (resultSet.getArray(name).array as IntArray).map { it.toByte() }.toList()},
        {resultSet, index -> (resultSet.getArray(index).array as IntArray).map { it.toByte() }.toList()},
        {statement, index, value -> statement.setArray(index, statement.connection.createArrayOf("Array(Int8)", value.toTypedArray())) },
        {DbInt8()})

class DbArrayUInt8 : DbIntArrayType<Byte>("Array(UInt8)",
        {resultSet, name -> (resultSet.getArray(name).array as LongArray).map { it.toByte() }.toList()},
        {resultSet, index -> (resultSet.getArray(index).array as LongArray).map { it.toByte() }.toList()},
        {statement, index, value -> statement.setArray(index, statement.connection.createArrayOf("Array(UInt8)", value.toTypedArray())) },
        {DbUInt8()})

class DbArrayInt16 : DbIntArrayType<Short>("Array(Int16)",
        {resultSet, name -> (resultSet.getArray(name).array as IntArray).map { it.toShort() }.toList()},
        {resultSet, index -> (resultSet.getArray(index).array as IntArray).map { it.toShort() }.toList()},
        {statement, index, value -> statement.setArray(index, statement.connection.createArrayOf("Array(Int16)", value.toTypedArray())) },
        {DbInt16()})

class DbArrayUInt16 : DbIntArrayType<Short>("Array(UInt16)",
        {resultSet, name -> (resultSet.getArray(name).array as LongArray).map { it.toShort() }.toList()},
        {resultSet, index -> (resultSet.getArray(index).array as LongArray).map { it.toShort() }.toList()},
        {statement, index, value -> statement.setArray(index, statement.connection.createArrayOf("Array(UInt16)", value.toTypedArray())) },
        {DbUInt16()})

class DbArrayInt32 : DbIntArrayType<Int>("Array(Int32)",
        {resultSet, name -> (resultSet.getArray(name).array as IntArray).toList()},
        {resultSet, index -> (resultSet.getArray(index).array as IntArray).toList()},
        {statement, index, value -> statement.setArray(index, statement.connection.createArrayOf("Array(Int32)", value.toTypedArray())) },
        {DbInt32()})

class DbArrayUInt32 : DbIntArrayType<Int>("Array(UInt32)",
        {resultSet, name -> (resultSet.getArray(name).array as LongArray).map { it.toInt() }.toList()},
        {resultSet, index -> (resultSet.getArray(index).array as LongArray).map { it.toInt() }.toList()},
        {statement, index, value -> statement.setArray(index, statement.connection.createArrayOf("Array(UInt32)", value.toTypedArray())) },
        {DbUInt32()})

class DbArrayInt64 : DbIntArrayType<Long>("Array(Int64)",
        {resultSet, name -> (resultSet.getArray(name).array as LongArray).toList()},
        {resultSet, index -> (resultSet.getArray(index).array as LongArray).toList()},
        {statement, index, value -> statement.setArray(index, statement.connection.createArrayOf("Array(Int64)", value.toTypedArray())) },
        {DbInt64()})

class DbArrayUInt64 : DbIntArrayType<Long>("Array(UInt64)",
        {resultSet, name -> (resultSet.getArray(name).array as Array<BigInteger>).map { it.toLong() }.toList()},
        {resultSet, index -> (resultSet.getArray(index).array as Array<BigInteger>).map { it.toLong() }.toList()},
        {statement, index, value -> statement.setArray(index, statement.connection.createArrayOf("Array(UInt64)", value.toTypedArray())) },
        {DbUInt64()})

//Boolean

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


//String

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
