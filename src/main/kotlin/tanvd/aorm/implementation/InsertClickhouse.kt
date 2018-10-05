package tanvd.aorm.implementation

import tanvd.aorm.Database
import tanvd.aorm.DbType
import tanvd.aorm.expression.Column
import tanvd.aorm.insert.InsertExpression
import tanvd.aorm.utils.use
import java.sql.Connection
import java.sql.PreparedStatement

object InsertClickhouse {
    fun insert(db: Database, expression: InsertExpression) {
        db.withConnection {
            constructInsert(expression).use {
                it.execute()
            }
        }
    }

    private fun Connection.constructInsert(insert: InsertExpression): PreparedStatement {
        val sql = "INSERT INTO ${insert.table.name} (${insert.columns.joinToString { it.name }}) VALUES " +
                insert.values.joinToString { "(${insert.columns.joinToString { "?" }})" }
        return prepareStatement(sql).let {
            var index = 1
            for (row in insert.values) {
                for (column in insert.columns) {
                    @Suppress("UNCHECKED_CAST")
                    val value = row[column as Column<Any, DbType<Any>>] ?: column.defaultValueResolved()
                    column.setValue(index, it, value)
                    index++
                }
            }
            it
        }
    }

    fun constructInsert(insert: InsertExpression): String {
        return "INSERT INTO ${insert.table.name} (${insert.columns.joinToString { it.name }}) VALUES " +
                "${insert.values.joinToString { row ->
                    insert.columns.joinToString(prefix = "(", postfix = ")") {
                        @Suppress("UNCHECKED_CAST")
                        val value = row[it as Column<Any, DbType<Any>>] ?: it.defaultValueResolved()
                        it.toStringValue(value)
                    }
                }};"
    }
}