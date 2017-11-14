package implementation.table

import utils.AormTestBase
import utils.AssertDb
import utils.ExampleTable
import org.testng.Assert
import org.testng.annotations.Test
import tanvd.aorm.Column
import tanvd.aorm.DbLong
import tanvd.aorm.exceptions.BasicDbException
import tanvd.aorm.implementation.MetadataClickhouse
import tanvd.aorm.implementation.TableClickhouse

class TableClickhouseTest : AormTestBase() {
    @Test
    fun createTable_tableNotExists_tableSyncedWithDb() {
        TableClickhouse.create(ExampleTable)

        AssertDb.syncedWithDb(ExampleTable)
    }

    @Test
    fun createTable_tableExists_gotBasicDbException() {
        TableClickhouse.create(ExampleTable)

        try {
            TableClickhouse.create(ExampleTable)
        } catch (e : BasicDbException) {
            return
        }

        Assert.fail()
    }

    @Test
    fun dropTable_tableExists_tableNotExistsInMetadata() {
        TableClickhouse.create(ExampleTable)

        TableClickhouse.drop(ExampleTable)

        Assert.assertFalse(MetadataClickhouse.existsTable(ExampleTable))
    }

    @Test
    fun createTable_tableNotExists_gotBasicDbException() {
        try {
            TableClickhouse.drop(ExampleTable)
        } catch (e : BasicDbException) {
            return
        }

        Assert.fail()
    }


    @Test
    fun addColumn_tableExists_columnAddedInMetadata() {
        TableClickhouse.create(ExampleTable)

        TableClickhouse.addColumn(ExampleTable, Column("new_column", DbLong()))

        val metadataColumns = MetadataClickhouse.columnsOfTable(ExampleTable)
        Assert.assertEquals(metadataColumns["new_column"]?.toLowerCase(), DbLong().toSqlName().toLowerCase())
    }

    @Test
    fun addColumn_tableNotExists_gotBasicDbException() {
        try {
            TableClickhouse.addColumn(ExampleTable, Column("new_column", DbLong()))
        } catch (e : BasicDbException) {
            return
        }

        Assert.fail()
    }


    @Test
    fun dropColumn_tableExists_columnNotInMetadata() {
        TableClickhouse.create(ExampleTable)

        TableClickhouse.dropColumn(ExampleTable, ExampleTable.value)

        val metadataColumns = MetadataClickhouse.columnsOfTable(ExampleTable)
        Assert.assertNull(metadataColumns[ExampleTable.value.name])
    }

    @Test
    fun dropColumn_tableNotExists_gotBasicDbException() {
        try {
            TableClickhouse.dropColumn(ExampleTable, ExampleTable.value)
        } catch (e : BasicDbException) {
            return
        }

        Assert.fail()
    }

}