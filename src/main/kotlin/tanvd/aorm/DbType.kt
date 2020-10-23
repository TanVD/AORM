package tanvd.aorm

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import ru.yandex.clickhouse.ClickHouseUtil
import java.math.BigDecimal
import java.math.BigInteger
import java.sql.*
import java.text.SimpleDateFormat
import java.util.Date
import kotlin.reflect.KClass
import kotlin.reflect.KFunction2
import kotlin.reflect.KFunction3

sealed class DbType<T> {
    abstract val defaultValue: T

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
    override val defaultValue = emptyList<T>()

    abstract fun toPrimitive(): DbPrimitiveType<T>
}

//Enums

abstract class DbEnum<T : Enum<*>>(val enumMapping: LinkedHashMap<String, Int>, val enumType: KClass<T>) : DbPrimitiveType<T>() {

    override val defaultValue: T = enumType.java.enumConstants.first()

    protected fun mappingToSql() = enumMapping.entries.joinToString { "\'${it.key}\' = ${it.value}" }

    override fun getValue(name: String, result: ResultSet): T = enumType.java.enumConstants.first { it.name == result.getString(name) }

    override fun getValue(index: Int, result: ResultSet): T = enumType.java.enumConstants.first { it.name == result.getString(index) }

    override fun setValue(index: Int, statement: PreparedStatement, value: T) {
        statement.setString(index, value.name)
    }

    override fun toStringValue(value: T): String = value.name

    override fun toArray(): DbArrayType<T> {
        throw NotImplementedError()
    }
}

class DbEnum8<T : Enum<*>>(enumMapping: LinkedHashMap<String, Int>, enumType: KClass<T>) : DbEnum<T>(enumMapping, enumType) {
    constructor(enumType: KClass<T>) : this(LinkedHashMap<String, Int>().also { map ->
        enumType.java.enumConstants.forEach {
            map[it.name] = it.ordinal
        }
    }, enumType)

    override fun toSqlName(): String = "Enum8(${mappingToSql()})"
}

class DbEnum16<T : Enum<*>>(enumMapping: LinkedHashMap<String, Int>, enumType: KClass<T>) : DbEnum<T>(enumMapping, enumType) {
    constructor(enumType: KClass<T>) : this(LinkedHashMap<String, Int>().also { map ->
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
        private val defaultDate = Date(0)
    }

    override val defaultValue: Date = defaultDate

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
        private val defaultDateTime = DateTime(0)
    }

    override val defaultValue: DateTime = defaultDateTime

    override fun toSqlName(): String = "DateTime"

    override fun getValue(name: String, result: ResultSet): DateTime = DateTime(result.getTimestamp(name).time)
    override fun getValue(index: Int, result: ResultSet): DateTime = DateTime(result.getTimestamp(index).time)

    override fun setValue(index: Int, statement: PreparedStatement, value: DateTime) {
        statement.setTimestamp(index, Timestamp(value.millis))
    }

    override fun toStringValue(value: DateTime): String = "'${dateTimeFormat.print(value)}'"

    override fun toArray(): DbArrayType<DateTime> {
        throw NotImplementedError()
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

abstract class DbNumericPrimitiveType<T : Number>(val sqlName: String,
                                                  override val defaultValue: T,
                                                  val getByNameFunc: KFunction2<ResultSet, String, T>,
                                                  val getByIndexFunc: KFunction2<ResultSet, Int, T>,
                                                  val setByIndexFunc: KFunction3<PreparedStatement, Int, T, Unit>,
                                                  val getArrayType: () -> DbArrayType<T>) : DbPrimitiveType<T>() {
    override fun toSqlName(): String = sqlName

    override fun getValue(name: String, result: ResultSet) = getByNameFunc(result, name)
    override fun getValue(index: Int, result: ResultSet) = getByIndexFunc(result, index)

    override fun setValue(index: Int, statement: PreparedStatement, value: T) {
        setByIndexFunc(statement, index, value)
    }

    override fun toStringValue(value: T): String = value.toString()

    override fun toArray() = getArrayType()

}

class DbInt8 : DbNumericPrimitiveType<Byte>("Int8", 0, ResultSet::getByte, ResultSet::getByte, PreparedStatement::setByte, { DbArrayInt8() })

class DbUInt8 : DbNumericPrimitiveType<Byte>("UInt8", 0, ResultSet::getByte, ResultSet::getByte, PreparedStatement::setByte, { DbArrayUInt8() })

class DbInt16 : DbNumericPrimitiveType<Short>("Int16", 0, ResultSet::getShort, ResultSet::getShort, PreparedStatement::setShort, { DbArrayInt16() })

class DbUInt16 : DbNumericPrimitiveType<Short>("UInt16", 0, ResultSet::getShort, ResultSet::getShort, PreparedStatement::setShort, { DbArrayInt16() })

class DbInt32 : DbNumericPrimitiveType<Int>("Int32", 0, ResultSet::getInt,ResultSet::getInt, PreparedStatement::setInt, { DbArrayInt32() })

class DbUInt32 : DbNumericPrimitiveType<Int>("UInt32", 0, ResultSet::getInt,ResultSet::getInt, PreparedStatement::setInt, { DbArrayInt32() })

class DbInt64 : DbNumericPrimitiveType<Long>("Int64", 0, ResultSet::getLong, ResultSet::getLong, PreparedStatement::setLong, { DbArrayInt64() })

class DbUInt64 : DbNumericPrimitiveType<Long>("UInt64", 0, ResultSet::getLong, ResultSet::getLong, PreparedStatement::setLong, { DbArrayInt64() })

class DbFloat32 : DbNumericPrimitiveType<Float>("Float32", 0f, ResultSet::getFloat, ResultSet::getFloat, PreparedStatement::setFloat, { DbArrayFloat32() })

class DbFloat64 : DbNumericPrimitiveType<Double>("Float64", 0.0, ResultSet::getDouble, ResultSet::getDouble, PreparedStatement::setDouble, { DbArrayFloat64() })

class DbDecimal(val bit: Int, val scale: Int) : DbNumericPrimitiveType<BigDecimal>(
    "Decimal$bit($scale)", BigDecimal.ZERO, ResultSet::getBigDecimal, ResultSet::getBigDecimal, PreparedStatement::setBigDecimal, { DbArrayDecimal(bit, scale) }
)

//Int array types

abstract class DbNumericArrayType<T : Number>(val sqlName: String,
                                              val getByNameFunc: (ResultSet, String) -> List<T>,
                                              val getByIndexFunc: (ResultSet, Int) -> List<T>,
                                              val getPrimitiveType: () -> DbPrimitiveType<T>,
                                              @Suppress("PLATFORM_CLASS_MAPPED_TO_KOTLIN")
                                              val setByIndexFunc: (PreparedStatement, Int, List<T>) -> Unit = { statement, index, value ->
                                                  statement.setArray(index, statement.connection.createArrayOf(sqlName, (value as java.util.Collection<*>).toArray()))
                                              }) : DbArrayType<T>() {
    override fun toSqlName(): String = sqlName

    override fun getValue(name: String, result: ResultSet) = getByNameFunc(result, name)
    override fun getValue(index: Int, result: ResultSet) = getByIndexFunc(result, index)

    override fun setValue(index: Int, statement: PreparedStatement, value: List<T>) {
        setByIndexFunc(statement, index, value)
    }

    override fun toStringValue(value: List<T>): String = value.joinToString(prefix = "[", postfix = "]") { it.toString() }

    override fun toPrimitive(): DbPrimitiveType<T> = getPrimitiveType()
}

class DbArrayInt8 : DbNumericArrayType<Byte>("Array(Int8)",
        { resultSet, name -> (resultSet.getArray(name).array as IntArray).map { it.toByte() } },
        { resultSet, index -> (resultSet.getArray(index).array as IntArray).map { it.toByte() } },
        { DbInt8() })

class DbArrayUInt8 : DbNumericArrayType<Byte>("Array(UInt8)",
        { resultSet, name -> (resultSet.getArray(name).array as LongArray).map { it.toByte() } },
        { resultSet, index -> (resultSet.getArray(index).array as IntArray).map { it.toByte() } },
        { DbUInt8() })

class DbArrayInt16 : DbNumericArrayType<Short>("Array(Int16)",
        { resultSet, name -> (resultSet.getArray(name).array as IntArray).map { it.toShort() } },
        { resultSet, index -> (resultSet.getArray(index).array as IntArray).map { it.toShort() } },
        { DbInt16() })

class DbArrayUInt16 : DbNumericArrayType<Short>("Array(UInt16)",
        { resultSet, name -> (resultSet.getArray(name).array as LongArray).map { it.toShort() } },
        { resultSet, index -> (resultSet.getArray(index).array as IntArray).map { it.toShort() } },
        { DbUInt16() })

class DbArrayInt32 : DbNumericArrayType<Int>("Array(Int32)",
        { resultSet, name -> (resultSet.getArray(name).array as IntArray).toList() },
        { resultSet, index -> (resultSet.getArray(index).array as IntArray).toList() },
        { DbInt32() })

class DbArrayUInt32 : DbNumericArrayType<Int>("Array(UInt32)",
        { resultSet, name -> (resultSet.getArray(name).array as LongArray).map { it.toInt() } },
        { resultSet, index -> (resultSet.getArray(index).array as LongArray).map { it.toInt() } },
        { DbUInt32() })

class DbArrayInt64 : DbNumericArrayType<Long>("Array(Int64)",
        { resultSet, name -> (resultSet.getArray(name).array as LongArray).toList() },
        { resultSet, index -> (resultSet.getArray(index).array as LongArray).toList() },
        { DbInt64() })

@Suppress("UNCHECKED_CAST")
class DbArrayUInt64 : DbNumericArrayType<Long>("Array(UInt64)",
        { resultSet, name -> (resultSet.getArray(name).array as Array<BigInteger>).map { it.toLong() } },
        { resultSet, index -> (resultSet.getArray(index).array as Array<BigInteger>).map { it.toLong() } },
        { DbUInt64() })

class DbArrayFloat32 : DbNumericArrayType<Float>("Array(Float32)",
        { resultSet, name -> (resultSet.getArray(name).array as FloatArray).toList() },
        { resultSet, index -> (resultSet.getArray(index).array as FloatArray).toList() },
        { DbFloat32() })

class DbArrayFloat64 : DbNumericArrayType<Double>("Array(Float64)",
        { resultSet, name -> (resultSet.getArray(name).array as DoubleArray).toList() },
        { resultSet, index -> (resultSet.getArray(index).array as DoubleArray).toList() },
        { DbFloat64() })

class DbArrayDecimal(private val bit: Int, val scale: Int) : DbNumericArrayType<BigDecimal>("Array(Decimal$bit($scale))",
    { resultSet, name -> (resultSet.getArray(name).array as Array<BigDecimal>).toList() },
    { resultSet, index -> (resultSet.getArray(index).array as Array<BigDecimal>).toList() },
    { DbDecimal(bit, scale) })

//Boolean

class DbBoolean : DbPrimitiveType<Boolean>() {

    override val defaultValue = false

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
        return (result.getArray(index).array as IntArray).map { it == 1 }
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

    override val defaultValue = ""

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
