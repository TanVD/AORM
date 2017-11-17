package implementation.table

import utils.AormTestBase
import utils.AssertDb
import utils.ExampleTable
import org.testng.Assert
import org.testng.annotations.Test
import tanvd.aorm.expression.Column
import tanvd.aorm.DbLong
import tanvd.aorm.DbType
import tanvd.aorm.implementation.MetadataClickhouse
import tanvd.aorm.implementation.TableClickhouse

@Suppress("UNCHECKED_CAST")
class MetadataClickhouseTest : AormTestBase() {
    @Test
    fun existsTable_tableNotExists_returnFalse() {
        Assert.assertFalse(MetadataClickhouse.existsTable(ExampleTable))
    }

    @Test
    fun existsTable_tableExists_returnTrue() {
        TableClickhouse.create(ExampleTable)

        Assert.assertTrue(MetadataClickhouse.existsTable(ExampleTable))
    }

    @Test
    fun columnsOfTable_tableNotExists_emptyMap() {
        Assert.assertTrue(MetadataClickhouse.columnsOfTable(ExampleTable).isEmpty())
    }

    @Test
    fun columnsOfTable_tableExists_mapOfColumnsOfTable() {
        TableClickhouse.create(ExampleTable)

        Assert.assertEquals(MetadataClickhouse.columnsOfTable(ExampleTable),
                ExampleTable.columns.map { it.name to it.type.toSqlName() }.toMap())
    }

    @Test
    fun syncScheme_tableNotExists_tableSyncedWithDb() {
        MetadataClickhouse.syncScheme(ExampleTable)

        AssertDb.syncedWithDb(ExampleTable)
    }

    @Test
    fun syncScheme_tableExistsNewColumn_columnAdded() {
        TableClickhouse.create(ExampleTable)
        val newColumn = Column("new_column", DbLong(), ExampleTable)
        ExampleTable.columns.add(newColumn as Column<Any, DbType<Any>>)

        MetadataClickhouse.syncScheme(ExampleTable)

        val metadataColumns = MetadataClickhouse.columnsOfTable(ExampleTable)
        Assert.assertTrue(metadataColumns.any {(name, type) -> name == newColumn.name && type == newColumn.type.toSqlName()})
    }

    @Test
    fun syncScheme_tableExistsRemoveColumn_columnsNotChanged() {
        TableClickhouse.create(ExampleTable)
        ExampleTable.columns.remove(ExampleTable.value as Column<Any, DbType<Any>>)

        MetadataClickhouse.syncScheme(ExampleTable)

        val metadataColumns = MetadataClickhouse.columnsOfTable(ExampleTable)
        Assert.assertTrue(metadataColumns.any {(name, type) -> name == ExampleTable.value.name &&
                type == ExampleTable.value.type.toSqlName()})
    }
}