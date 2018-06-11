package tanvd.aorm.context

import tanvd.aorm.*
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
    fun Table.create() = TableClickhouse.create(db, this)

    fun Table.drop() = TableClickhouse.drop(db, this)

    fun Table.exists(): Boolean = TableClickhouse.exists(db, this)

    @Suppress("UNCHECKED_CAST")
    fun <E : Any, T : DbType<E>> Table.addColumn(column: Column<E, T>) {
        if (!columns.contains(column as Column<Any, DbType<Any>>)) {
            TableClickhouse.addColumn(db, this, column)
            columns.add(column)
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun <E : Any, T : DbType<E>> Table.dropColumn(column: Column<E, T>) {
        if (columns.contains(column as Column<Any, DbType<Any>>)) {
            TableClickhouse.dropColumn(db, this, column)
            columns.remove(column)
        }
    }

    fun Table.syncScheme() = MetadataClickhouse.syncScheme(db, this)

    //DML
    //selects
    fun Table.select(): Query {
        @Suppress("UNCHECKED_CAST")
        return Query(this, columns)
    }

    @Suppress("UNCHECKED_CAST")
    fun Table.select(vararg functions: Expression<*, DbType<*>>): Query = Query(this, functions.toList() as List<Expression<Any, DbType<Any>>>)

    fun Query.toResult(): List<SelectRow> = QueryClickhouse.getResult(db, this)


    //inserts
    @Suppress("UNCHECKED_CAST")
    fun Table.insert(body: (InsertRow) -> Unit) {
        val row = InsertRow()
        body(row)
        InsertClickhouse.insert(db, InsertExpression(this, row))
    }

    @Suppress("UNCHECKED_CAST")
    fun <T : Any> Table.batchInsert(list: Iterable<T>, columns: List<Column<*, DbType<*>>> = this.columns,
                                    body: (InsertRow, T) -> Unit) {
        val rows = ArrayList<InsertRow>()
        list.forEach {
            val row = InsertRow()
            body(row, it)
            rows.add(row)
        }
        InsertClickhouse.insert(db, InsertExpression(this, columns as List<Column<Any, DbType<Any>>>, rows))
    }
}

