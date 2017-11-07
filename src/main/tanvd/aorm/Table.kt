package tanvd.aorm

import tanvd.aorm.implementation.InsertClickhouse
import tanvd.aorm.implementation.MetadataClickhouse
import tanvd.aorm.implementation.TableClickhouse
import tanvd.aorm.query.Query
import tanvd.aorm.query.QueryFunction
import tanvd.aorm.query.SimpleColumn
import java.util.*

abstract class Table(val name: String) {
    abstract var db: Database

    abstract val engine: Engine

    val columns: MutableList<Column<Any, DbType<Any>>> = ArrayList()

    val columnsWithDefaults
        get() = columns.filter { it.defaultFunction != null }

    private fun <T, E: DbType<T>>registerColumn(column: Column<T, E>): Column<T, E> {
        columns.add(column as Column<Any, DbType<Any>>)
        return column
    }

    fun date(name: String) = registerColumn(Column(name, DbDate()))

    fun datetime(name: String) = registerColumn(Column(name, DbDateTime()))

    fun ulong(name: String) = registerColumn(Column(name, DbULong()))

    fun long(name: String) = registerColumn(Column(name, DbLong()))

    fun boolean(name: String) = registerColumn(Column(name, DbBoolean()))

    fun string(name: String) = registerColumn(Column(name, DbString()))


    fun arrayDate(name: String) = registerColumn(Column(name, DbArrayDate()))

//    fun arrayDateTime(name: String) = registerColumn(Column(name, DbArrayDateTime()))

    fun arrayULong(name: String) = registerColumn(Column(name, DbArrayULong()))

    fun arrayLong(name: String) = registerColumn(Column(name, DbArrayLong()))

    fun arrayBoolean(name: String) = registerColumn(Column(name, DbArrayBoolean()))

    fun arrayString(name: String) = registerColumn(Column(name, DbArrayString()))



    //Table functions
    fun create() {
        TableClickhouse.create(this)
    }

    fun drop() {
        TableClickhouse.drop(this)
    }

    fun <E: Any, T: DbType<E>>addColumn(column: Column<E, T>, useDDL : Boolean = true) {
        if (!columns.contains(column as Column<Any, DbType<Any>>)) {
            if (useDDL) {
                TableClickhouse.addColumn(this, column)
            }
            columns.add(column)
        }
    }

    fun <E: Any, T: DbType<E>>dropColumn(column: Column<E, T>, useDDL : Boolean = true) {
        if (columns.contains(column as Column<Any, DbType<Any>>)) {
            if (useDDL) {
                TableClickhouse.dropColumn(this, column)
            }
            columns.remove(column)
        }
    }

    fun select(): Query {
        @Suppress("UNCHECKED_CAST")
        return Query(this, columns.map { SimpleColumn(it) })
    }

    fun select(vararg functions: QueryFunction) : Query {
        return Query(this, functions.toList())
    }


    fun syncScheme() {
        MetadataClickhouse.syncScheme(this)
    }


    fun insert(expression: InsertExpression) {
        InsertClickhouse.insert(expression)
    }
}