package tanvd.aorm.utils

import org.testng.Assert
import tanvd.aorm.*
import tanvd.aorm.implementation.MetadataClickhouse

object AssertDb {
    fun syncedWithDb(database: Database, table: Table) {
        Assert.assertTrue(MetadataClickhouse.existsTable(database, table))

        val metadataColumns = MetadataClickhouse.columnsOfTable(database, table)

        for ((name, type) in metadataColumns) {
            Assert.assertTrue(table.columns.any { column -> column.name == name && column.type.toSqlName() == type })
        }

        for (column in table.columns) {
            Assert.assertTrue(metadataColumns.any { (name, type) -> name == column.name && type == column.type.toSqlName() })
        }
    }

    fun assertEquals(selectRow: SelectRow, insertRow: InsertRow) {
        Assert.assertEquals(insertRow.values, selectRow.values)
    }

    fun assertEquals(selectRow: Set<SelectRow>, insertRow: Set<InsertRow>) {
        Assert.assertEquals(insertRow.map { it.values }.toSet(), selectRow.map { it.values }.toSet())
    }

    fun assertEquals(selectRow: List<SelectRow>, insertRow: List<InsertRow>) {
        Assert.assertEquals(insertRow.map { it.values }, selectRow.map { it.values })
    }
}
