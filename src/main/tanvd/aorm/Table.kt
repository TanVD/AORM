package tanvd.aorm

import tanvd.aorm.implementation.InsertClickhouse
import tanvd.aorm.implementation.MetadataClickhouse
import tanvd.aorm.implementation.TableClickhouse
import tanvd.aorm.query.Query
import tanvd.aorm.query.QueryFunction
import tanvd.aorm.query.SimpleColumn
import java.util.*


abstract class Table(var name: String, var db: Database) {
    open val useDDL: Boolean = true

    abstract val engine: Engine

    val columns: MutableList<Column<Any, DbType<Any>>> = ArrayList()

    val columnsWithDefaults
        get() = columns.filter { it.defaultFunction != null }

    @Suppress("UNCHECKED_CAST")
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
    fun create() = ddlRequest {
        TableClickhouse.create(this)
    }

    fun drop() = ddlRequest {
        TableClickhouse.drop(this)
    }

    @Suppress("UNCHECKED_CAST")
    fun <E: Any, T: DbType<E>>addColumn(column: Column<E, T>) {
        if (!columns.contains(column as Column<Any, DbType<Any>>)) {
            ddlRequest {
                TableClickhouse.addColumn(this, column)
            }
            columns.add(column)
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun <E: Any, T: DbType<E>>dropColumn(column: Column<E, T>) = ddlRequest {
        if (columns.contains(column as Column<Any, DbType<Any>>)) {
            ddlRequest {
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


    fun syncScheme() = ddlRequest{
        MetadataClickhouse.syncScheme(this)
    }

    @Suppress("UNCHECKED_CAST")
    fun insert(body: (MutableMap<Column<*, DbType<*>>, Any>) -> Unit) {
        val rowMap = HashMap<Column<*, DbType<*>>, Any>()
        body(rowMap)
        InsertClickhouse.insert(InsertExpression(this, Row(rowMap as Map<Column<Any, DbType<Any>>, Any>)))
    }

    @Suppress("UNCHECKED_CAST")
    fun <T: Any>batchInsert(list: Iterable<T>, columns: List<Column<*, DbType<*>>> = this.columns,
                            body: (MutableMap<Column<*, DbType<*>>, Any>, T) -> Unit) {
        val rows = ArrayList<Row>()
        list.forEach {
            val rowMap = HashMap<Column<*, DbType<*>>, Any>()
            body(rowMap, it)
            rows.add(Row(rowMap as Map<Column<Any, DbType<Any>>, Any>))
        }
        InsertClickhouse.insert(InsertExpression(this, columns as List<Column<Any, DbType<Any>>>, rows))
    }

    private fun ddlRequest(body: () -> Unit) {
        if (useDDL) {
            body()
        }
    }
}