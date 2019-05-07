package tanvd.aorm.utils

import org.junit.jupiter.api.Assertions
import tanvd.aorm.*
import tanvd.aorm.implementation.MetadataClickhouse

object AssertDb {
    fun syncedWithDb(database: Database, table: Table) {
        Assertions.assertTrue(MetadataClickhouse.existsTable(database, table))

        val metadataColumns = MetadataClickhouse.columnsOfTable(database, table)

        for ((name, type) in metadataColumns) {
            Assertions.assertTrue(table.columns.any { column -> column.name == name && column.type.toSqlName() == type })
        }

        for (column in table.columns) {
            Assertions.assertTrue(metadataColumns.any { (name, type) -> name == column.name && type == column.type.toSqlName() })
        }
    }

    fun assertEquals(selectRow: SelectRow, insertRow: InsertRow) {
        Assertions.assertEquals(insertRow.values, selectRow.values)
    }

    fun assertEquals(selectRow: Set<SelectRow>, insertRow: Set<InsertRow>) {
        Assertions.assertEquals(insertRow.map { it.values }.toSet(), selectRow.map { it.values }.toSet())
    }

    fun assertEquals(selectRow: List<SelectRow>, insertRow: List<InsertRow>) {
        Assertions.assertEquals(insertRow.map { it.values }, selectRow.map { it.values })
    }
}
