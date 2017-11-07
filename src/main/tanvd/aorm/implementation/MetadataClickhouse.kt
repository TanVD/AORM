package tanvd.aorm.implementation

import ru.yandex.clickhouse.ClickHouseConnection
import ru.yandex.clickhouse.ClickHouseDatabaseMetadata
import tanvd.aorm.Table

object MetadataClickhouse {
    fun syncScheme(table: Table) {
        if (!existsTable(table)) {
            TableClickhouse.create(table)
            return
        }
        val dbTableColumns = columnsOfTable(table)

        for (column in table.columns) {
            val exists = dbTableColumns.any { (name, type) -> name.equals(column.name, true)
                    && type.equals(column.type.toSqlName(), true)}
            if (!exists) {
                TableClickhouse.addColumn(table, column)
            }
        }
    }

    fun existsTable(table: Table) : Boolean {
        return table.db.withConnection {
            val metadata = ClickHouseDatabaseMetadata(table.db.url, this as ClickHouseConnection)
            metadata.getTables(null, table.db.name, table.name, null).use {
                it.next()
            }
        }
    }

    /**
     * Returns columns in JDBC representation.
     * Names to Types
     */
    fun columnsOfTable(table: Table) : Map<String, String> {
        return table.db.withConnection {
            val metadata = ClickHouseDatabaseMetadata(table.db.url, this as ClickHouseConnection)
            metadata.getColumns(null, table.db.name, table.name, null).use {
                val tableColumns = HashMap<String, String>()
                while (it.next()) {
                    tableColumns.put(it.getString("COLUMN_NAME"), it.getString("TYPE_NAME"))
                }
                tableColumns
            }
        }
    }
}