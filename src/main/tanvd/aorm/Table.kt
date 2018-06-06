package tanvd.aorm

import tanvd.aorm.expression.Column
import java.util.*
import kotlin.reflect.KClass


abstract class Table(var name: String) {
    val columns: MutableList<Column<Any, DbType<Any>>> = ArrayList()

    abstract val engine: Engine

    val columnsWithDefaults
        get() = columns.filter { it.defaultFunction != null }

    @Suppress("UNCHECKED_CAST")
    private fun <T, E : DbType<T>> registerColumn(column: Column<T, E>): Column<T, E> {
        columns.add(column as Column<Any, DbType<Any>>)
        return column
    }

    //date
    fun date(name: String) = registerColumn(Column(name, DbDate(), this))
    fun arrayDate(name: String) = registerColumn(Column(name, DbArrayDate(), this))

    //datetime
    fun datetime(name: String) = registerColumn(Column(name, DbDateTime(), this))
//    fun arrayDateTime(name: String) = registerColumn(Column(name, DbArrayDateTime()))

    //enums
    fun <T: Enum<*>> enum8(name: String, enumType: KClass<T>) = registerColumn(Column(name, DbEnum8(enumType), this))
    fun <T: Enum<*>> enum16(name: String, enumType: KClass<T>) = registerColumn(Column(name, DbEnum16(enumType), this))

    //int8
    fun int8(name: String) = registerColumn(Column(name, DbInt8(), this))
    fun arrayInt8(name: String) = registerColumn(Column(name, DbArrayInt8(), this))

    fun uint8(name: String) = registerColumn(Column(name, DbUInt8(), this))
    fun arrayUInt8(name: String) = registerColumn(Column(name, DbArrayUInt8(), this))

    //int16
    fun int16(name: String) = registerColumn(Column(name, DbInt16(), this))
    fun arrayInt16(name: String) = registerColumn(Column(name, DbArrayInt16(), this))

    fun uint16(name: String) = registerColumn(Column(name, DbUInt16(), this))
    fun arrayUInt16(name: String) = registerColumn(Column(name, DbArrayUInt16(), this))

    //int32
    fun int32(name: String) = registerColumn(Column(name, DbInt32(), this))
    fun arrayInt32(name: String) = registerColumn(Column(name, DbArrayInt32(), this))

    fun uint32(name: String) = registerColumn(Column(name, DbUInt32(), this))
    fun arrayUInt32(name: String) = registerColumn(Column(name, DbArrayUInt32(), this))

    //int64
    fun int64(name: String) = registerColumn(Column(name, DbInt64(), this))
    fun arrayInt64(name: String) = registerColumn(Column(name, DbArrayInt64(), this))

    fun uint64(name: String) = registerColumn(Column(name, DbUInt64(), this))
    fun arrayUInt64(name: String) = registerColumn(Column(name, DbArrayUInt64(), this))

    //boolean
    fun boolean(name: String) = registerColumn(Column(name, DbBoolean(), this))
    fun arrayBoolean(name: String) = registerColumn(Column(name, DbArrayBoolean(), this))

    //string
    fun string(name: String) = registerColumn(Column(name, DbString(), this))
    fun arrayString(name: String) = registerColumn(Column(name, DbArrayString(), this))
}