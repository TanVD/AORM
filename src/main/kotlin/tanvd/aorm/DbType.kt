package tanvd.aorm

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import tanvd.aorm.utils.escapeQuotes
import java.math.BigDecimal
import java.math.BigInteger
import java.sql.Date
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Types
import java.time.LocalDateTime
import java.time.ZonedDateTime
import java.util.Collection
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

        return toSqlName() == other.toSqlName()
    }

    override fun hashCode(): Int = toSqlName().hashCode()
}

sealed class DbPrimitiveType<T> : DbType<T>() {
    abstract fun toArray(): DbArrayType<T>
}


@Suppress("UNCHECKED_CAST")
private fun <R> Any.asTypedList() = (this as Array<*>).toList() as List<R>
private fun <R> ResultSet.getTypedList(index: Int) = getArray(index).array.asTypedList<R>()
private fun <R> ResultSet.getTypedList(name: String) = getArray(name).array.asTypedList<R>()

sealed class DbArrayType<T> : DbType<List<T>>() {
    override val defaultValue = emptyList<T>()

    override fun getValue(name: String, result: ResultSet): List<T> = result.getTypedList(name)
    override fun getValue(index: Int, result: ResultSet): List<T> = result.getTypedList(index)

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
    override val defaultValue: Date = Date(0)

    override fun toSqlName(): String = "Date"
    override fun getValue(name: String, result: ResultSet): Date = result.getDate(name)
    override fun getValue(index: Int, result: ResultSet): Date = result.getDate(index)
    override fun setValue(index: Int, statement: PreparedStatement, value: Date) {
        statement.setObject(index, value)
    }

    override fun toStringValue(value: Date): String = "'$value'"

    override fun toArray(): DbArrayType<Date> = DbArrayDate()
}

class DbArrayDate : DbArrayType<Date>() {
    override fun toSqlName(): String = "Array(Date)"

    @Suppress("UNCHECKED_CAST")
    override fun getValue(name: String, result: ResultSet): List<Date> =
        result.getTypedList<ZonedDateTime>(name).map {
            Date(it.toInstant().toEpochMilli())
        }

    @Suppress("UNCHECKED_CAST")
    override fun getValue(index: Int, result: ResultSet): List<Date> =
        result.getTypedList<ZonedDateTime>(index).map {
            Date(it.toInstant().toEpochMilli())
        }


    override fun setValue(index: Int, statement: PreparedStatement, value: List<Date>) {
        if (value.isNotEmpty()) {
            val array = value.toTypedArray()
            statement.setArray(index, statement.connection.createArrayOf(toPrimitive().toSqlName(), array))
        } else {
            statement.setNull(index, Types.ARRAY)
        }
    }

    override fun toStringValue(value: List<Date>): String =
            value.joinToString(prefix = "[", postfix = "]") { "'$it'" }

    override fun toPrimitive(): DbPrimitiveType<Date> = DbDate()
}

class DbDateTime : DbPrimitiveType<DateTime>() {
    companion object {
        val dateTimeFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
        private val defaultDateTime = DateTime(0)
    }

    override val defaultValue: DateTime = defaultDateTime

    override fun toSqlName(): String = "DateTime"

    private fun localDateTimeToDateTime(localDateTime : LocalDateTime) : DateTime = with(localDateTime) {
        DateTime(year, monthValue, dayOfMonth, hour, minute, second)
    }

    override fun getValue(name: String, result: ResultSet): DateTime =
            localDateTimeToDateTime(result.getObject(name, LocalDateTime::class.java))

    override fun getValue(index: Int, result: ResultSet): DateTime =
            localDateTimeToDateTime(result.getObject(index, LocalDateTime::class.java))

    override fun setValue(index: Int, statement: PreparedStatement, value: DateTime) {
        val localDateTime = with(value) {
            LocalDateTime.of(year, monthOfYear, dayOfMonth, hourOfDay, minuteOfHour, secondOfMinute)
        }
        statement.setObject(index, localDateTime)
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

class DbUInt8 : DbNumericPrimitiveType<Short>("UInt8", 0, ResultSet::getShort, ResultSet::getShort, PreparedStatement::setShort, { DbArrayUInt8() })

class DbInt16 : DbNumericPrimitiveType<Short>("Int16", 0, ResultSet::getShort, ResultSet::getShort, PreparedStatement::setShort, { DbArrayInt16() })

class DbUInt16 : DbNumericPrimitiveType<Int>("UInt16", 0, ResultSet::getInt, ResultSet::getInt, PreparedStatement::setInt, { DbArrayUInt16() })

class DbInt32 : DbNumericPrimitiveType<Int>("Int32", 0, ResultSet::getInt, ResultSet::getInt, PreparedStatement::setInt, { DbArrayInt32() })

class DbUInt32 : DbNumericPrimitiveType<Long>("UInt32", 0, ResultSet::getLong, ResultSet::getLong, PreparedStatement::setLong, { DbArrayUInt32() })

class DbInt64 : DbNumericPrimitiveType<Long>("Int64", 0, ResultSet::getLong, ResultSet::getLong, PreparedStatement::setLong, { DbArrayInt64() })

class DbUInt64 : DbNumericPrimitiveType<BigInteger>("UInt64", BigInteger.ZERO, ResultSet::getBigInteger, ResultSet::getBigInteger, PreparedStatement::setBigInteger, { DbArrayUInt64() })

class DbFloat32 : DbNumericPrimitiveType<Float>("Float32", 0f, ResultSet::getFloat, ResultSet::getFloat, PreparedStatement::setFloat, { DbArrayFloat32() })

class DbFloat64 : DbNumericPrimitiveType<Double>("Float64", 0.0, ResultSet::getDouble, ResultSet::getDouble, PreparedStatement::setDouble, { DbArrayFloat64() })

class DbDecimal(val bit: Int, val scale: Int) : DbNumericPrimitiveType<BigDecimal>(
    "Decimal$bit($scale)", BigDecimal.ZERO, ResultSet::getBigDecimal, ResultSet::getBigDecimal, PreparedStatement::setBigDecimal, { DbArrayDecimal(bit, scale) }
)

private fun ResultSet.getBigInteger(columnIndex: Int): BigInteger = getBigDecimal(columnIndex).unscaledValue()
private fun ResultSet.getBigInteger(columnLabel: String): BigInteger = getBigDecimal(columnLabel).unscaledValue()
private fun PreparedStatement.setBigInteger(index: Int, x: BigInteger) = setBigDecimal(index, BigDecimal(x))

//Int array types

abstract class DbNumericArrayType<T : Number>(
    val sqlName: String,
    val getPrimitiveType: () -> DbPrimitiveType<T>,
    val arraySqlTypeName: String = getPrimitiveType().toSqlName(),
    val getByNameFunc: (ResultSet, String) -> List<T> = { resultSet, name -> resultSet.getTypedList(name) },
    val getByIndexFunc: (ResultSet, Int) -> List<T> = { resultSet, index -> resultSet.getTypedList(index) },
    val setByIndexFunc: (PreparedStatement, Int, List<T>) -> Unit = { statement, index, value ->
        if (value.isNotEmpty()) {
            statement.setArray(
                index,
                statement.connection.createArrayOf(arraySqlTypeName, (value as Collection<*>).toArray())
            )
        } else {
            statement.setNull(index, Types.ARRAY)
        }
    },
) : DbArrayType<T>() {
    override fun toSqlName(): String = sqlName

    override fun getValue(name: String, result: ResultSet) = getByNameFunc(result, name)
    override fun getValue(index: Int, result: ResultSet) = getByIndexFunc(result, index)

    override fun setValue(index: Int, statement: PreparedStatement, value: List<T>) {
        if (value.isNotEmpty()) {
            setByIndexFunc(statement, index, value)
        } else {
            statement.setNull(index, Types.NUMERIC)
        }
    }

    override fun toStringValue(value: List<T>): String =
        value.joinToString(prefix = "[", postfix = "]") { it.toString() }

    override fun toPrimitive(): DbPrimitiveType<T> = getPrimitiveType()
}

class DbArrayInt8 : DbNumericArrayType<Byte>("Array(Int8)", { DbInt8() })

class DbArrayUInt8 : DbNumericArrayType<Short>("Array(UInt8)", { DbUInt8() })

class DbArrayInt16 : DbNumericArrayType<Short>("Array(Int16)", { DbInt16() })

class DbArrayUInt16 : DbNumericArrayType<Int>("Array(UInt16)", { DbUInt16() })

class DbArrayInt32 : DbNumericArrayType<Int>("Array(Int32)", { DbInt32() })

class DbArrayUInt32 : DbNumericArrayType<Long>("Array(UInt32)", { DbUInt32() })

class DbArrayInt64 : DbNumericArrayType<Long>("Array(Int64)", { DbInt64() })

class DbArrayUInt64 : DbNumericArrayType<BigInteger>("Array(UInt64)", { DbUInt64() })

class DbArrayFloat32 : DbNumericArrayType<Float>("Array(Float32)", { DbFloat32() })

class DbArrayFloat64 : DbNumericArrayType<Double>("Array(Float64)", { DbFloat64() })

@Suppress("UNCHECKED_CAST")
class DbArrayDecimal(private val bit: Int, val scale: Int) : DbNumericArrayType<BigDecimal>(
    sqlName = "Array(Decimal$bit($scale))",
    getPrimitiveType = { DbDecimal(bit, scale) },
    arraySqlTypeName = "Decimal$bit"
)

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
        return result.getTypedList<Short>(name).map { it == 1.toShort() }
    }

    override fun getValue(index: Int, result: ResultSet): List<Boolean> {
        return result.getTypedList<Short>(index).map { it == 1.toShort() }
    }

    override fun setValue(index: Int, statement: PreparedStatement, value: List<Boolean>) {
        if (value.isNotEmpty()) {
            val array = value.map { if (it) 1 else 0 }.toTypedArray()
            statement.setArray(index, statement.connection.createArrayOf(toPrimitive().toSqlName(), array))
        } else {
            statement.setNull(index, Types.ARRAY)
        }
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

    override fun toStringValue(value: String): String = "'${value.escapeQuotes()}'"

    override fun toArray(): DbArrayType<String> = DbArrayString()
}

class DbArrayString : DbArrayType<String>() {
    override fun toSqlName(): String = "Array(String)"

    override fun setValue(index: Int, statement: PreparedStatement, value: List<String>) {
        if (value.isNotEmpty()) {
            statement.setArray(index,
                statement.connection.createArrayOf(toPrimitive().toSqlName(), value.toTypedArray()))
        } else {
            statement.setNull(index, Types.ARRAY)
        }
    }

    override fun toStringValue(value: List<String>): String =
            value.joinToString(prefix = "[", postfix = "]") { "'${it.escapeQuotes()}'" }

    override fun toPrimitive(): DbPrimitiveType<String> = DbString()
}
