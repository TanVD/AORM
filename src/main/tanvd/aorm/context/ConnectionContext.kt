package tanvd.aorm.context

import tanvd.aorm.*
import tanvd.aorm.exceptions.NoValueInsertedException
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

    fun <E : Any, T : DbType<E>> Table.addColumn(column: Column<E, T>) {
        if (!columns.contains(column)) {
            TableClickhouse.addColumn(db, this, column)
            columns.add(column)
        }
    }

    fun <E : Any, T : DbType<E>> Table.dropColumn(column: Column<E, T>) {
        if (columns.contains(column)) {
            TableClickhouse.dropColumn(db, this, column)
            columns.remove(column)
        }
    }

    fun Table.syncScheme() = MetadataClickhouse.syncScheme(db, this)

    //DML
    //selects
    fun Table.select(): Query {
        return Query(this, columns)
    }

    fun Table.select(vararg functions: Expression<*, DbType<*>>): Query = Query(this, functions.toSet())

    fun Query.toResult(): List<SelectRow> = QueryClickhouse.getResult(db, this)


    //inserts
    fun Table.insert(body: (InsertRow) -> Unit) {
        val row = InsertRow()
        body(row)
        InsertClickhouse.insert(db, InsertExpression(this, row))
    }

    @Throws(NoValueInsertedException::class)
    fun <T : Any> Table.batchInsert(list: Iterable<T>, columns: Set<Column<*, DbType<*>>>? = null,
                                    body: (InsertRow, T) -> Unit) {
        val listIterator = list.iterator()
        if (!listIterator.hasNext()) return

        val rows = ArrayList<InsertRow>()
        val columnsFromRows = columns.orEmpty().toMutableSet()
        listIterator.forEach {
            val row = InsertRow()
            body(row, it)
            columnsFromRows.addAll(row.columns)
            rows.add(row)
        }

        if (columnsFromRows.isEmpty()) {
            throw NoValueInsertedException("No values was inserted during batch insert")
        }

        InsertClickhouse.insert(db, InsertExpression(this, columnsFromRows, rows))
    }
}

