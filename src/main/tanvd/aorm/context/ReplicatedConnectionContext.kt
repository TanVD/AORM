package tanvd.aorm.context

import tanvd.aorm.*
import tanvd.aorm.expression.Column
import tanvd.aorm.expression.Expression
import tanvd.aorm.implementation.MetadataClickhouse
import tanvd.aorm.implementation.TableClickhouse
import tanvd.aorm.query.Query

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

    fun <E : Any, T : DbType<E>> Table.addColumn(column: Column<E, T>, specificDb: Database? = null) {
        val dbToChoose = chooseDb(specificDb)

        with(ConnectionContext(dbToChoose)) {
            addColumn(column)
        }
    }

    fun <E : Any, T : DbType<E>> Table.dropColumn(column: Column<E, T>, specificDb: Database? = null) {
        val dbToChoose = chooseDb(specificDb)
        with(ConnectionContext(dbToChoose)) {
            dropColumn(column)
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

    fun Table.select(vararg functions: Expression<*, DbType<*>>): Query = Query(this, functions.toSet())

    fun Query.toResult(specificDb: Database? = null): List<SelectRow> {
        val dbToChoose = chooseDb(specificDb)
        return with(ConnectionContext(dbToChoose)) {
            toResult()
        }
    }

    //inserts
    fun Table.insert(specificDb: Database? = null, body: (InsertRow) -> Unit) {
        val dbToChoose = chooseDb(specificDb)
        with(ConnectionContext(dbToChoose)) {
            insert(body)
        }
    }

    fun <T : Any> Table.batchInsert(list: Iterable<T>, columns: Set<Column<*, DbType<*>>>? = null,
                                    specificDb: Database? = null,
                                    body: (InsertRow, T) -> Unit) {
        val dbToChoose = chooseDb(specificDb)
        with(ConnectionContext(dbToChoose)) {
            batchInsert(list, columns, body)
        }
    }

    private fun chooseDb(database: Database?): Database = database ?: dbs.first().first
}