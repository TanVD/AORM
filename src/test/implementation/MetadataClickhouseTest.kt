package implementation

import utils.AormTestBase
import utils.AssertDb
import utils.ExampleTable
import org.powermock.core.classloader.annotations.PowerMockIgnore
import org.powermock.core.classloader.annotations.PrepareForTest
import org.testng.Assert
import org.testng.annotations.Test
import tanvd.aorm.Column
import tanvd.aorm.DbLong
import tanvd.aorm.DbType
import tanvd.aorm.implementation.MetadataClickhouse
import tanvd.aorm.implementation.TableClickhouse

@PowerMockIgnore("javax.management.*", "javax.xml.parsers.*", "com.sun.org.apache.xerces.internal.jaxp.*", "ch.qos.logback.*", "org.slf4j.*")
@PrepareForTest(TableClickhouse::class)
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
        val newColumn = Column("new_column", DbLong())
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