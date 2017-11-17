package tanvd.aorm.implementation

import tanvd.aorm.Table

object MetadataClickhouse {
    fun syncScheme(table: Table) {
        if (!existsTable(table)) {
            TableClickhouse.create(table)
            return
        }
        val dbTableColumns = columnsOfTable(table)

        table.columns.filter { column ->
            !dbTableColumns.any { (name, type) ->
                name == column.name
                        && type == column.type.toSqlName()
            }
        }.forEach { column ->
            TableClickhouse.addColumn(table, column)
        }
    }

    fun existsTable(table: Table): Boolean {
        return table.db.withConnection {
            metaData.getTables(null, table.db.name, table.name, null).use {
                it.next()
            }
        }
    }

    /**
     * Returns columns in JDBC representation.
     * Names to Types
     */
    fun columnsOfTable(table: Table): Map<String, String> {
        return table.db.withConnection {
            metaData.getColumns(null, table.db.name, table.name, null).use {
                val tableColumns = HashMap<String, String>()
                while (it.next()) {
                    tableColumns.put(it.getString("COLUMN_NAME"), it.getString("TYPE_NAME"))
                }
                tableColumns
            }
        }
    }
}