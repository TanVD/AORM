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

class ReplicatedConnectionContext(val dbs: List<Pair<Database, Int>>) {

    //DDL
    fun Table.create() {
        dbs.forEach { (db, index) ->
            TableClickhouse.replicatedCreate(db, index, this)
        }
    }

    fun Table.drop() {
        dbs.forEach { (db, _) ->
            TableClickhouse.drop(db, this)
        }
    }

    fun Table.exists(): List<Pair<Database, Boolean>> = dbs.map { (db, _) ->
        db to TableClickhouse.exists(db, this)
    }

    @Suppress("UNCHECKED_CAST")
    fun <E : Any, T : DbType<E>> Table.addColumn(column: Column<E, T>, specificDb: Database? = null) {
        val dbToChoose = chooseDb(specificDb)
        if (!columns.contains(column as Column<Any, DbType<Any>>)) {
            TableClickhouse.addColumn(dbToChoose, this, column)
            columns.add(column)
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun <E : Any, T : DbType<E>> Table.dropColumn(column: Column<E, T>, specificDb: Database? = null) {
        val dbToChoose = chooseDb(specificDb)
        if (columns.contains(column as Column<Any, DbType<Any>>)) {
            TableClickhouse.dropColumn(dbToChoose, this, column)
            columns.remove(column)
        }
    }

    fun Table.syncScheme(specificDb: Database? = null) {
        val existingTables = this.exists()
        existingTables.forEach { (db, existsTable) ->
            if (!existsTable) {
                val index = dbs.single { it.first == db }.second
                TableClickhouse.replicatedCreate(db, index, this)
            }
        }
        val dbToChoose = chooseDb(specificDb)
        MetadataClickhouse.syncScheme(dbToChoose, this)
    }

    //DML
    //selects
    fun Table.select(): Query {
        return Query(this, columns)
    }

    @Suppress("UNCHECKED_CAST")
    fun Table.select(vararg functions: Expression<*, DbType<*>>): Query = Query(this, functions.toList() as List<Expression<Any, DbType<Any>>>)

    fun Query.toResult(specificDb: Database? = null): List<SelectRow> {
        val dbToChoose = chooseDb(specificDb)
        return QueryClickhouse.getResult(dbToChoose, this)
    }

    //inserts
    @Suppress("UNCHECKED_CAST")
    fun Table.insert(specificDb: Database? = null, body: (InsertRow) -> Unit) {
        val dbToChoose = chooseDb(specificDb)
        val row = InsertRow()
        body(row)
        InsertClickhouse.insert(dbToChoose, InsertExpression(this, row))
    }

    @Suppress("UNCHECKED_CAST")
    fun <T : Any> Table.batchInsert(list: Iterable<T>, columns: List<Column<*, DbType<*>>> = this.columns,
                                    specificDb: Database? = null,
                                    body: (InsertRow, T) -> Unit) {
        val dbToChoose = chooseDb(specificDb)
        val rows = ArrayList<InsertRow>()
        list.forEach {
            val row = InsertRow()
            body(row, it)
            rows.add(row)
        }
        InsertClickhouse.insert(dbToChoose, InsertExpression(this, columns as List<Column<Any, DbType<Any>>>, rows))
    }

    private fun chooseDb(database: Database?): Database = database ?: dbs.first().first
}