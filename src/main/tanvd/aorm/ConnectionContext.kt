package tanvd.aorm

import tanvd.aorm.expression.Column
import tanvd.aorm.expression.Expression
import tanvd.aorm.implementation.InsertClickhouse
import tanvd.aorm.implementation.MetadataClickhouse
import tanvd.aorm.implementation.QueryClickhouse
import tanvd.aorm.implementation.TableClickhouse
import tanvd.aorm.query.Query
import java.util.*

class ConnectionContext(val db: Database) {

    //DDL
    fun Table.create() = TableClickhouse.create(this@ConnectionContext.db, this)

    fun Table.drop() = TableClickhouse.drop(this@ConnectionContext.db, this)

    fun Table.exists(): Boolean = TableClickhouse.exists(this@ConnectionContext.db, this)

    @Suppress("UNCHECKED_CAST")
    fun <E : Any, T : DbType<E>> Table.addColumn(column: Column<E, T>) {
        if (!columns.contains(column as Column<Any, DbType<Any>>)) {
            TableClickhouse.addColumn(this@ConnectionContext.db, this, column)
            columns.add(column)
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun <E : Any, T : DbType<E>> Table.dropColumn(column: Column<E, T>) {
        if (columns.contains(column as Column<Any, DbType<Any>>)) {
            TableClickhouse.dropColumn(this@ConnectionContext.db, this, column)
            columns.remove(column)
        }
    }

    fun Table.syncScheme() = MetadataClickhouse.syncScheme(this@ConnectionContext.db, this)

    //DML
    //selects
    fun Table.select(): Query {
        @Suppress("UNCHECKED_CAST")
        return Query(this, columns)
    }

    @Suppress("UNCHECKED_CAST")
    fun Table.select(vararg functions: Expression<*, DbType<*>>): Query = Query(this, functions.toList() as List<Expression<Any, DbType<Any>>>)

    fun Query.toResult(): List<Row> = QueryClickhouse.getResult(this@ConnectionContext.db, this)


    //inserts
    @Suppress("UNCHECKED_CAST")
    fun Table.insert(body: (Row) -> Unit) {
        val row = Row()
        body(row)
        InsertClickhouse.insert(this@ConnectionContext.db, InsertExpression(this, row))
    }

    @Suppress("UNCHECKED_CAST")
    fun <T : Any> Table.batchInsert(list: Iterable<T>, columns: List<Column<*, DbType<*>>> = this.columns,
                                    body: (Row, T) -> Unit) {
        val rows = ArrayList<Row>()
        list.forEach {
            val row = Row()
            body(row, it)
            rows.add(row)
        }
        InsertClickhouse.insert(this@ConnectionContext.db, InsertExpression(this, columns as List<Column<Any, DbType<Any>>>, rows))
    }
}

