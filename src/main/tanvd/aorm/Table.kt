package tanvd.aorm

import tanvd.aorm.expression.Column
import tanvd.aorm.expression.Expression
import tanvd.aorm.implementation.InsertClickhouse
import tanvd.aorm.implementation.MetadataClickhouse
import tanvd.aorm.implementation.TableClickhouse
import tanvd.aorm.query.Query
import java.util.*


abstract class Table(var name: String, var db: Database) {
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

    fun ulong(name: String) = registerColumn(Column(name, DbULong(), this))

    fun long(name: String) = registerColumn(Column(name, DbLong(), this))

    fun boolean(name: String) = registerColumn(Column(name, DbBoolean(), this))

    fun string(name: String) = registerColumn(Column(name, DbString(), this))


    fun arrayDate(name: String) = registerColumn(Column(name, DbArrayDate(), this))

//    fun arrayDateTime(name: String) = registerColumn(Column(name, DbArrayDateTime()))

    fun arrayULong(name: String) = registerColumn(Column(name, DbArrayULong(), this))

    fun arrayLong(name: String) = registerColumn(Column(name, DbArrayLong(), this))

    fun arrayBoolean(name: String) = registerColumn(Column(name, DbArrayBoolean(), this))

    fun arrayString(name: String) = registerColumn(Column(name, DbArrayString(), this))


    //Table functions
    fun create() = TableClickhouse.create(this)

    fun drop() = TableClickhouse.drop(this)

    fun exists(): Boolean = TableClickhouse.exists(this)

    @Suppress("UNCHECKED_CAST")
    fun <E : Any, T : DbType<E>> addColumn(column: Column<E, T>) {
        if (!columns.contains(column as Column<Any, DbType<Any>>)) {
            TableClickhouse.addColumn(this, column)
            columns.add(column)
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun <E : Any, T : DbType<E>> dropColumn(column: Column<E, T>) {
        if (columns.contains(column as Column<Any, DbType<Any>>)) {
            TableClickhouse.dropColumn(this, column)
            columns.remove(column)
        }
    }

    fun select(): Query {
        @Suppress("UNCHECKED_CAST")
        return Query(this, columns)
    }

    @Suppress("UNCHECKED_CAST")
    fun select(vararg functions: Expression<*, DbType<*>>): Query = Query(this, functions.toList() as List<Expression<Any, DbType<Any>>>)


    fun syncScheme() = MetadataClickhouse.syncScheme(this)

    @Suppress("UNCHECKED_CAST")
    fun insert(body: (MutableMap<Column<*, DbType<*>>, Any>) -> Unit) {
        val rowMap = HashMap<Column<*, DbType<*>>, Any>()
        body(rowMap)
        InsertClickhouse.insert(InsertExpression(this, Row(rowMap as Map<Column<Any, DbType<Any>>, Any>)))
    }

    @Suppress("UNCHECKED_CAST")
    fun <T : Any> batchInsert(list: Iterable<T>, columns: List<Column<*, DbType<*>>> = this.columns,
                              body: (MutableMap<Column<*, DbType<*>>, Any>, T) -> Unit) {
        val rows = ArrayList<Row<Column<Any, DbType<Any>>>>()
        list.forEach {
            val rowMap = HashMap<Column<*, DbType<*>>, Any>()
            body(rowMap, it)
            rows.add(Row(rowMap as Map<Column<Any, DbType<Any>>, Any>))
        }
        InsertClickhouse.insert(InsertExpression(this, columns as List<Column<Any, DbType<Any>>>, rows))
    }
}