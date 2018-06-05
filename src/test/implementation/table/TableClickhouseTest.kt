package implementation.table

import org.testng.Assert
import org.testng.annotations.Test
import tanvd.aorm.DbInt64
import tanvd.aorm.exceptions.BasicDbException
import tanvd.aorm.expression.Column
import tanvd.aorm.implementation.MetadataClickhouse
import tanvd.aorm.implementation.TableClickhouse
import utils.AormTestBase
import utils.AssertDb
import utils.ExampleTable
import utils.TestDatabase

class TableClickhouseTest : AormTestBase() {
    @Test
    fun createTable_tableNotExists_tableSyncedWithDb() {
        TableClickhouse.create(TestDatabase, ExampleTable)

        AssertDb.syncedWithDb(ExampleTable)
    }

    @Test
    fun createTable_tableExists_gotBasicDbException() {
        TableClickhouse.create(TestDatabase, ExampleTable)

        try {
            TableClickhouse.create(TestDatabase, ExampleTable)
        } catch (e: BasicDbException) {
            return
        }

        Assert.fail()
    }

    @Test
    fun dropTable_tableExists_tableNotExistsInMetadata() {
        TableClickhouse.create(TestDatabase, ExampleTable)

        TableClickhouse.drop(TestDatabase, ExampleTable)

        Assert.assertFalse(MetadataClickhouse.existsTable(TestDatabase, ExampleTable))
    }

    @Test
    fun createTable_tableNotExists_gotBasicDbException() {
        try {
            TableClickhouse.drop(TestDatabase, ExampleTable)
        } catch (e: BasicDbException) {
            return
        }

        Assert.fail()
    }


    @Test
    fun addColumn_tableExists_columnAddedInMetadata() {
        TableClickhouse.create(TestDatabase, ExampleTable)

        TableClickhouse.addColumn(TestDatabase, ExampleTable, Column("new_column", DbInt64(), ExampleTable))

        val metadataColumns = MetadataClickhouse.columnsOfTable(TestDatabase, ExampleTable)
        Assert.assertEquals(metadataColumns["new_column"]?.toLowerCase(), DbInt64().toSqlName().toLowerCase())
    }

    @Test
    fun addColumn_tableNotExists_gotBasicDbException() {
        try {
            TableClickhouse.addColumn(TestDatabase, ExampleTable, Column("new_column", DbInt64(), ExampleTable))
        } catch (e: BasicDbException) {
            return
        }

        Assert.fail()
    }


    @Test
    fun dropColumn_tableExists_columnNotInMetadata() {
        TableClickhouse.create(TestDatabase, ExampleTable)

        TableClickhouse.dropColumn(TestDatabase, ExampleTable, ExampleTable.value)

        val metadataColumns = MetadataClickhouse.columnsOfTable(TestDatabase, ExampleTable)
        Assert.assertNull(metadataColumns[ExampleTable.value.name])
    }

    @Test
    fun dropColumn_tableNotExists_gotBasicDbException() {
        try {
            TableClickhouse.dropColumn(TestDatabase, ExampleTable, ExampleTable.value)
        } catch (e: BasicDbException) {
            return
        }

        Assert.fail()
    }

}