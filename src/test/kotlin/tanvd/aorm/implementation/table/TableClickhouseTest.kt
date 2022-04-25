package tanvd.aorm.implementation.table

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import tanvd.aorm.DbInt64
import tanvd.aorm.exceptions.BasicDbException
import tanvd.aorm.expression.Column
import tanvd.aorm.implementation.MetadataClickhouse
import tanvd.aorm.implementation.TableClickhouse
import tanvd.aorm.utils.*

class TableClickhouseTest : AormTestBase() {
    @Test
    fun createTable_tableNotExists_tableSyncedWithDb() {
        TableClickhouse.create(database, ExampleTable)

        AssertDb.syncedWithDb(database, ExampleTable)
    }

    @Test
    fun createTable_tableExists_gotBasicDbException() {
        TableClickhouse.create(database, ExampleTable)

        try {
            TableClickhouse.create(database, ExampleTable)
        } catch (e: BasicDbException) {
            return
        }

        Assertions.fail<Nothing>()
    }

    @Test
    fun dropTable_tableExists_tableNotExistsInMetadata() {
        TableClickhouse.create(database, ExampleTable)

        TableClickhouse.drop(database, ExampleTable)

        Assertions.assertFalse(MetadataClickhouse.existsTable(database, ExampleTable))
    }

    @Test
    fun createTable_tableNotExists_gotBasicDbException() {
        try {
            TableClickhouse.drop(database, ExampleTable)
        } catch (e: BasicDbException) {
            return
        }

        Assertions.fail<Nothing>()
    }


    @Test
    fun addColumn_tableExists_columnAddedInMetadata() {
        TableClickhouse.create(database, ExampleTable)

        TableClickhouse.addColumn(database, ExampleTable, Column("new_column", DbInt64(), ExampleTable))

        val metadataColumns = MetadataClickhouse.columnsOfTable(database, ExampleTable)
        Assertions.assertEquals(metadataColumns["new_column"]?.lowercase(), DbInt64().toSqlName().lowercase())
    }

    @Test
    fun addColumn_tableNotExists_gotBasicDbException() {
        try {
            TableClickhouse.addColumn(database, ExampleTable, Column("new_column", DbInt64(), ExampleTable))
        } catch (e: BasicDbException) {
            return
        }

        Assertions.fail<Nothing>()
    }


    @Test
    fun dropColumn_tableExists_columnNotInMetadata() {
        TableClickhouse.create(database, ExampleTable)

        TableClickhouse.dropColumn(database, ExampleTable, ExampleTable.value)

        val metadataColumns = MetadataClickhouse.columnsOfTable(database, ExampleTable)
        Assertions.assertNull(metadataColumns[ExampleTable.value.name])
    }

    @Test
    fun dropColumn_tableNotExists_gotBasicDbException() {
        try {
            TableClickhouse.dropColumn(database, ExampleTable, ExampleTable.value)
        } catch (e: BasicDbException) {
            return
        }

        Assertions.fail<Nothing>()
    }

}
