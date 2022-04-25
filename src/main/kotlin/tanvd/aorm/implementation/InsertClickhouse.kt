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
            constructBatchInsert(expression).use(PreparedStatement::executeBatch)
        }
    }

    private fun Connection.constructBatchInsert(insert: InsertExpression): PreparedStatement {
        val columns = insert.columns
        val sql = "INSERT INTO ${insert.table.name} (${columns.joinToString { it.name }}) VALUES " +
                "(${columns.joinToString { "?" }})"
        return prepareStatement(sql).also { statement ->
            insert.values.forEach { row ->
                columns.forEachIndexed { index, column ->
                    val valueIndex = index + 1

                    @Suppress("UNCHECKED_CAST")
                    val value = row[column as Column<Any, DbType<Any>>] ?: column.defaultValueResolved()
                    column.setValue(valueIndex, statement, value)
                }
                statement.addBatch()
            }
        }
    }

    fun constructInsert(insert: InsertExpression): String =
        "INSERT INTO ${insert.table.name} (${insert.columns.joinToString { it.name }}) VALUES " +
                insert.values.joinToString { row ->
                    insert.columns.joinToString(prefix = "(", postfix = ")") {
                        @Suppress("UNCHECKED_CAST")
                        val value = row[it as Column<Any, DbType<Any>>] ?: it.defaultValueResolved()
                        it.toStringValue(value)
                    }
                }
}