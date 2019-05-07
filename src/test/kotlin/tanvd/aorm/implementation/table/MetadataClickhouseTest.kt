package tanvd.aorm.implementation.table

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import tanvd.aorm.DbInt64
import tanvd.aorm.expression.Column
import tanvd.aorm.implementation.MetadataClickhouse
import tanvd.aorm.implementation.TableClickhouse
import tanvd.aorm.utils.*

class MetadataClickhouseTest : AormTestBase() {
    @Test
    fun existsTable_tableNotExists_returnFalse() {
        Assertions.assertFalse(MetadataClickhouse.existsTable(database, ExampleTable))
    }

    @Test
    fun existsTable_tableExists_returnTrue() {
        TableClickhouse.create(database, ExampleTable)

        Assertions.assertTrue(MetadataClickhouse.existsTable(database, ExampleTable))
    }

    @Test
    fun columnsOfTable_tableNotExists_emptyMap() {
        Assertions.assertTrue(MetadataClickhouse.columnsOfTable(database, ExampleTable).isEmpty())
    }

    @Test
    fun columnsOfTable_tableExists_mapOfColumnsOfTable() {
        TableClickhouse.create(database, ExampleTable)

        Assertions.assertEquals(MetadataClickhouse.columnsOfTable(database, ExampleTable),
                ExampleTable.columns.map { it.name to it.type.toSqlName() }.toMap())
    }

    @Test
    fun syncScheme_tableNotExists_tableSyncedWithDb() {
        MetadataClickhouse.syncScheme(database, ExampleTable)

        AssertDb.syncedWithDb(database, ExampleTable)
    }

    @Test
    fun syncScheme_tableExistsNewColumn_columnAdded() {
        TableClickhouse.create(database, ExampleTable)
        val newColumn = Column("new_column", DbInt64(), ExampleTable)
        ExampleTable.columns.add(newColumn)

        MetadataClickhouse.syncScheme(database, ExampleTable)

        val metadataColumns = MetadataClickhouse.columnsOfTable(database, ExampleTable)
        Assertions.assertTrue(metadataColumns.any { (name, type) -> name == newColumn.name && type == newColumn.type.toSqlName() })
    }

    @Test
    fun syncScheme_tableExistsRemoveColumncolumnsNotChanged() {
        TableClickhouse.create(database, ExampleTable)
        ExampleTable.columns.remove(ExampleTable.value)

        MetadataClickhouse.syncScheme(database, ExampleTable)

        val metadataColumns = MetadataClickhouse.columnsOfTable(database, ExampleTable)
        Assertions.assertTrue(metadataColumns.any { (name, type) ->
            name == ExampleTable.value.name &&
                    type == ExampleTable.value.type.toSqlName()
        })
    }
}
