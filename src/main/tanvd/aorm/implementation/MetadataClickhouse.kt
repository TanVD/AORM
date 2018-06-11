package tanvd.aorm.implementation

import tanvd.aorm.Database
import tanvd.aorm.Table

object MetadataClickhouse {
    fun syncScheme(db: Database, table: Table) {
        if (!existsTable(db, table)) {
            TableClickhouse.create(db, table)
            return
        }
        val dbTableColumns = columnsOfTable(db, table)

        table.columns.filter { column ->
            !dbTableColumns.any { (name, type) ->
                name == column.name
                        && type == column.type.toSqlName()
            }
        }.forEach { column ->
            TableClickhouse.addColumn(db, table, column)
        }
    }

    fun existsTable(db: Database, table: Table): Boolean {
        return db.withConnection {
            metaData.getTables(null, db.name, table.name, null).use {
                it.next()
            }
        }
    }

    /**
     * Returns columns in JDBC representation.
     * Names to Types
     */
    fun columnsOfTable(db: Database, table: Table): Map<String, String> {
        return db.withConnection {
            metaData.getColumns(null, db.name, table.name, null).use {
                val tableColumns = HashMap<String, String>()
                while (it.next()) {
                    tableColumns[it.getString("COLUMN_NAME")] = it.getString("TYPE_NAME")
                }
                tableColumns
            }
        }
    }
}