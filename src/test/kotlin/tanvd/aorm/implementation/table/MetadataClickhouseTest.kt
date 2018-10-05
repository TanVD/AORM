package tanvd.aorm.implementation.table

import org.testng.Assert
import org.testng.annotations.Test
import tanvd.aorm.DbInt64
import tanvd.aorm.expression.Column
import tanvd.aorm.implementation.MetadataClickhouse
import tanvd.aorm.implementation.TableClickhouse
import tanvd.aorm.utils.AormTestBase
import tanvd.aorm.utils.AssertDb
import tanvd.aorm.utils.ExampleTable
import tanvd.aorm.utils.TestDatabase

class MetadataClickhouseTest : AormTestBase() {
    @Test
    fun existsTable_tableNotExists_returnFalse() {
        Assert.assertFalse(MetadataClickhouse.existsTable(TestDatabase, ExampleTable))
    }

    @Test
    fun existsTable_tableExists_returnTrue() {
        TableClickhouse.create(TestDatabase, ExampleTable)

        Assert.assertTrue(MetadataClickhouse.existsTable(TestDatabase, ExampleTable))
    }

    @Test
    fun columnsOfTable_tableNotExists_emptyMap() {
        Assert.assertTrue(MetadataClickhouse.columnsOfTable(TestDatabase, ExampleTable).isEmpty())
    }

    @Test
    fun columnsOfTable_tableExists_mapOfColumnsOfTable() {
        TableClickhouse.create(TestDatabase, ExampleTable)

        Assert.assertEquals(MetadataClickhouse.columnsOfTable(TestDatabase, ExampleTable),
                ExampleTable.columns.map { it.name to it.type.toSqlName() }.toMap())
    }

    @Test
    fun syncScheme_tableNotExists_tableSyncedWithDb() {
        MetadataClickhouse.syncScheme(TestDatabase, ExampleTable)

        AssertDb.syncedWithDb(ExampleTable)
    }

    @Test
    fun syncScheme_tableExistsNewColumn_columnAdded() {
        TableClickhouse.create(TestDatabase, ExampleTable)
        val newColumn = Column("new_column", DbInt64(), ExampleTable)
        ExampleTable.columns.add(newColumn)

        MetadataClickhouse.syncScheme(TestDatabase, ExampleTable)

        val metadataColumns = MetadataClickhouse.columnsOfTable(TestDatabase, ExampleTable)
        Assert.assertTrue(metadataColumns.any { (name, type) -> name == newColumn.name && type == newColumn.type.toSqlName() })
    }

    @Test
    fun syncScheme_tableExistsRemoveColumncolumnsNotChanged() {
        TableClickhouse.create(TestDatabase, ExampleTable)
        ExampleTable.columns.remove(ExampleTable.value)

        MetadataClickhouse.syncScheme(TestDatabase, ExampleTable)

        val metadataColumns = MetadataClickhouse.columnsOfTable(TestDatabase, ExampleTable)
        Assert.assertTrue(metadataColumns.any { (name, type) ->
            name == ExampleTable.value.name &&
                    type == ExampleTable.value.type.toSqlName()
        })
    }
}