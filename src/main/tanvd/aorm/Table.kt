package tanvd.aorm

import tanvd.aorm.expression.Column
import java.util.*


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

    fun date(name: String) = registerColumn(Column(name, DbDate(), this))

    fun datetime(name: String) = registerColumn(Column(name, DbDateTime(), this))

    fun int32(name: String) = registerColumn(Column(name, DbInt32(), this))

    fun uint64(name: String) = registerColumn(Column(name, DbUInt64(), this))

    fun int64(name: String) = registerColumn(Column(name, DbInt64(), this))

    fun boolean(name: String) = registerColumn(Column(name, DbBoolean(), this))

    fun string(name: String) = registerColumn(Column(name, DbString(), this))


    fun arrayDate(name: String) = registerColumn(Column(name, DbArrayDate(), this))

//    fun arrayDateTime(name: String) = registerColumn(Column(name, DbArrayDateTime()))

    fun arrayUInt64(name: String) = registerColumn(Column(name, DbArrayUInt64(), this))

    fun arrayInt64(name: String) = registerColumn(Column(name, DbArrayInt64(), this))

    fun arrayBoolean(name: String) = registerColumn(Column(name, DbArrayBoolean(), this))

    fun arrayString(name: String) = registerColumn(Column(name, DbArrayString(), this))


    //Table functions
}