package tanvd.aorm.context

import tanvd.aorm.*
import tanvd.aorm.exceptions.NoValueInsertedException
import tanvd.aorm.expression.Column
import tanvd.aorm.expression.Expression
import tanvd.aorm.implementation.*
import tanvd.aorm.query.Query
import java.util.*

class ConnectionContext(val db: Database) {
    //Table
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
        val columnsFromRows = (columns.orEmpty() + columnsWithDefaults).toMutableSet()
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

    //View
    fun View.create() = ViewClickhouse.create(db, this)
    fun View.drop() = ViewClickhouse.drop(db, this)
    fun View.exists(): Boolean = ViewClickhouse.exists(db, this)

    fun View.select(): Query =  Query(this.name, this.query.expressions)
    fun View.select(vararg functions: Expression<*, DbType<*>>): Query = Query(this.name, functions.toSet())

    //MaterializedView
    fun MaterializedView.create() = MaterializedViewClickhouse.create(db, this)
    fun MaterializedView.drop() = MaterializedViewClickhouse.drop(db, this)
    fun MaterializedView.exists(): Boolean = MaterializedViewClickhouse.exists(db, this)

    fun MaterializedView.select(): Query =  Query(this.name, this.query.expressions)
    fun MaterializedView.select(vararg functions: Expression<*, DbType<*>>): Query = Query(this.name, functions.toSet())
}

