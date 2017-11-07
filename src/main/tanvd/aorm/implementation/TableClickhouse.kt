package tanvd.aorm.implementation

import tanvd.aorm.Column
import tanvd.aorm.DbType
import tanvd.aorm.Table

object TableClickhouse {
    fun create(table: Table) {
        table.db.execute("CREATE TABLE ${table.name} " +
                "(${table.columns.joinToString { it.toSqlDef() }}) " +
                "ENGINE = ${table.engine.toSqlDef()};")
    }

    fun drop(table: Table) {
        table.db.execute("DROP TABLE ${table.name};")
    }

    fun addColumn(table: Table, column: Column<*, DbType<*>>) {
        table.db.execute("ALTER TABLE ${table.name} ADD COLUMN ${column.toSqlDef()};")
    }

    fun dropColumn(table: Table, column: Column<*, DbType<*>>) {
        table.db.execute("ALTER TABLE ${table.name} DROP COLUMN ${column.name};")
    }
}