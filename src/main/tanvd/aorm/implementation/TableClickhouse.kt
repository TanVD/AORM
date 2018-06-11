package tanvd.aorm.implementation

import tanvd.aorm.Database
import tanvd.aorm.DbType
import tanvd.aorm.Table
import tanvd.aorm.expression.Column

object TableClickhouse {
    fun create(db: Database, table: Table) {
        db.execute("CREATE TABLE ${table.name} " +
                "(${table.columns.joinToString { it.toSqlDef() }}) " +
                "ENGINE = ${table.engine.toSqlDef()};")
    }

    fun replicatedCreate(db: Database, index: Int, table: Table) {
        db.execute("CREATE TABLE ${table.name} " +
                "(${table.columns.joinToString { it.toSqlDef() }}) " +
                "ENGINE = ${table.engine.toReplicatedSqlDef(index)};")
    }

    fun exists(db: Database, table: Table): Boolean = MetadataClickhouse.existsTable(db, table)

    fun drop(db: Database, table: Table) {
        db.execute("DROP TABLE ${table.name};")
    }

    fun addColumn(db: Database, table: Table, column: Column<*, DbType<*>>) {
        db.execute("ALTER TABLE ${table.name} ADD COLUMN ${column.toSqlDef()};")
    }

    fun dropColumn(db: Database, table: Table, column: Column<*, DbType<*>>) {
        db.execute("ALTER TABLE ${table.name} DROP COLUMN ${column.name};")
    }
}