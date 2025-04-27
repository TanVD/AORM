package tanvd.aorm

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import tanvd.aorm.utils.escapeQuotes
import java.math.BigDecimal
import java.math.BigInteger
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Types
import java.time.LocalDateTime
import java.time.ZonedDateTime
import java.util.Collection
import java.util.Date
import kotlin.collections.isNotEmpty
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
private fun Date.asSQLDate() = java.sql.Date(time)

class DbDate : DbPrimitiveType<Date>() {
    override val defaultValue: Date = Date(0)
    override fun toSqlName(): String = "Date"
    override fun getValue(name: String, result: ResultSet): Date = result.getDate(name)
    override fun getValue(index: Int, result: ResultSet): Date = result.getDate(index)
    override fun setValue(index: Int, statement: PreparedStatement, value: Date) {
        statement.setObject(index, value.asSQLDate())
    }

    override fun toStringValue(value: Date): String = "'${value.asSQLDate()}'"

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
            val array = value.map { it.asSQLDate() }.toTypedArray()
            statement.setArray(index, statement.connection.createArrayOf(toPrimitive().toSqlName(), array))
        } else {
            statement.setNull(index, Types.ARRAY)
        }
    }

    override fun toStringValue(value: List<Date>): String =
            value.joinToString(prefix = "[", postfix = "]") { "'${it.asSQLDate()}'" }

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

@Suppress("UNCHECKED_CAST")
sealed class DbNumericPrimitiveType<DB : Number, CODE : Number>(
    val sqlName: String,
    override val defaultValue: CODE,
    val getByNameFunc: KFunction2<ResultSet, String, DB>,
    val getByIndexFunc: KFunction2<ResultSet, Int, DB>,
    val setByIndexFunc: KFunction3<PreparedStatement, Int, DB, Unit>,
    val getArrayType: () -> DbNumericArrayType<DB, CODE>,
    val dbToCodeConverter: DB.() -> CODE = { this as CODE },
    val codeToDbConverter: CODE.() -> DB = { this as DB },
) : DbPrimitiveType<CODE>() {
    override fun toSqlName(): String = sqlName

    override fun getValue(name: String, result: ResultSet) = getByNameFunc(result, name).dbToCodeConverter()
    override fun getValue(index: Int, result: ResultSet) = getByIndexFunc(result, index).dbToCodeConverter()

    override fun setValue(index: Int, statement: PreparedStatement, value: CODE) {
        setByIndexFunc(statement, index, value.codeToDbConverter())
    }

    override fun toStringValue(value: CODE): String = value.toString()

    override fun toArray() = getArrayType()

}

object DbInt8 : DbNumericPrimitiveType<Byte, Byte>("Int8", 0, ResultSet::getByte, ResultSet::getByte, PreparedStatement::setByte, { DbArrayInt8 })

object DbUInt8 : DbNumericPrimitiveType<Short, Byte>("UInt8", 0, ResultSet::getShort, ResultSet::getShort, PreparedStatement::setShort, { DbArrayUInt8 }, Short::toByte, Byte::toShort)

object DbInt16 : DbNumericPrimitiveType<Short, Short>("Int16", 0, ResultSet::getShort, ResultSet::getShort, PreparedStatement::setShort, { DbArrayInt16 })

object DbUInt16 : DbNumericPrimitiveType<Int, Short>("UInt16", 0, ResultSet::getInt, ResultSet::getInt, PreparedStatement::setInt, { DbArrayUInt16 }, Int::toShort, Short::toInt)

object DbInt32 : DbNumericPrimitiveType<Int, Int>("Int32", 0, ResultSet::getInt, ResultSet::getInt, PreparedStatement::setInt, { DbArrayInt32 })

object DbUInt32 : DbNumericPrimitiveType<Long, Int>("UInt32", 0, ResultSet::getLong, ResultSet::getLong, PreparedStatement::setLong, { DbArrayUInt32 }, Long::toInt, Int::toLong)

object DbInt64 : DbNumericPrimitiveType<Long, Long>("Int64", 0, ResultSet::getLong, ResultSet::getLong, PreparedStatement::setLong, { DbArrayInt64 })

object DbUInt64 : DbNumericPrimitiveType<BigInteger, Long>("UInt64", 0L, ResultSet::getBigInteger, ResultSet::getBigInteger, PreparedStatement::setBigInteger, { DbArrayUInt64 }, BigInteger::toLong, Long::toBigInteger)

object DbFloat32 : DbNumericPrimitiveType<Float, Float>("Float32", 0f, ResultSet::getFloat, ResultSet::getFloat, PreparedStatement::setFloat, { DbArrayFloat32 })

object DbFloat64 : DbNumericPrimitiveType<Double, Double>("Float64", 0.0, ResultSet::getDouble, ResultSet::getDouble, PreparedStatement::setDouble, { DbArrayFloat64 })

class DbDecimal(val bit: Int, val scale: Int) : DbNumericPrimitiveType<BigDecimal, BigDecimal>(
    "Decimal$bit($scale)", BigDecimal.ZERO, ResultSet::getBigDecimal, ResultSet::getBigDecimal, PreparedStatement::setBigDecimal, { DbArrayDecimal(bit, scale) }
)

private fun ResultSet.getBigInteger(columnIndex: Int): BigInteger = getBigDecimal(columnIndex).unscaledValue()
private fun ResultSet.getBigInteger(columnLabel: String): BigInteger = getBigDecimal(columnLabel).unscaledValue()
private fun PreparedStatement.setBigInteger(index: Int, x: BigInteger) = setBigDecimal(index, BigDecimal(x))

//Int array types

abstract class DbNumericArrayType<DB : Number, CODE : Number>(
    val sqlName: String,
    val getPrimitiveType: () -> DbNumericPrimitiveType<DB, CODE>,
    val arraySqlTypeName: String = getPrimitiveType().toSqlName(),
    val getByNameFunc: (ResultSet, String) -> List<DB> = { resultSet, name -> resultSet.getTypedList(name) },
    val getByIndexFunc: (ResultSet, Int) -> List<DB> = { resultSet, index -> resultSet.getTypedList(index) },
    val setByIndexFunc: (PreparedStatement, Int, List<CODE>) -> Unit = { statement, index, value ->
        if (value.isNotEmpty()) {
            statement.setArray(
                index,
                statement.connection.createArrayOf(arraySqlTypeName, (value as Collection<*>).toArray())
            )
        } else {
            statement.setNull(index, Types.ARRAY)
        }
    },
) : DbArrayType<CODE>() {
    override fun toSqlName(): String = sqlName

    final override fun getValue(name: String, result: ResultSet) = getByNameFunc(result, name).map { getPrimitiveType().dbToCodeConverter(it) }
    final override fun getValue(index: Int, result: ResultSet) = getByIndexFunc(result, index).map { getPrimitiveType().dbToCodeConverter(it) }

    override fun setValue(index: Int, statement: PreparedStatement, value: List<CODE>) {
        if (value.isNotEmpty()) {
            setByIndexFunc(statement, index, value)
        } else {
            statement.setNull(index, Types.NUMERIC)
        }
    }

    override fun toStringValue(value: List<CODE>): String =
        value.joinToString(prefix = "[", postfix = "]") { it.toString() }

    override fun toPrimitive(): DbNumericPrimitiveType<DB, CODE> = getPrimitiveType()
}

object DbArrayInt8 : DbNumericArrayType<Byte, Byte>("Array(Int8)", { DbInt8 })

object DbArrayUInt8 : DbNumericArrayType<Short, Byte>("Array(UInt8)", { DbUInt8 })

object DbArrayInt16 : DbNumericArrayType<Short, Short>("Array(Int16)", { DbInt16 })

object DbArrayUInt16 : DbNumericArrayType<Int, Short>("Array(UInt16)", { DbUInt16 })

object DbArrayInt32 : DbNumericArrayType<Int, Int>("Array(Int32)", { DbInt32 })

object DbArrayUInt32 : DbNumericArrayType<Long, Int>("Array(UInt32)", { DbUInt32 })

object DbArrayInt64 : DbNumericArrayType<Long, Long>("Array(Int64)", { DbInt64 })

object DbArrayUInt64 : DbNumericArrayType<BigInteger, Long>("Array(UInt64)", { DbUInt64 })

object DbArrayFloat32 : DbNumericArrayType<Float, Float>("Array(Float32)", { DbFloat32 })

object DbArrayFloat64 : DbNumericArrayType<Double, Double>("Array(Float64)", { DbFloat64 })

@Suppress("UNCHECKED_CAST")
class DbArrayDecimal(private val bit: Int, val scale: Int) : DbNumericArrayType<BigDecimal, BigDecimal>(
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
