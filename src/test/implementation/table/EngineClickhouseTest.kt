package implementation.table

import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.aorm.*
import tanvd.aorm.implementation.TableClickhouse
import utils.AssertDb
import utils.TestDatabase
import utils.ignoringExceptions

class EngineClickhouseTest  {

    @BeforeMethod
    fun resetTables() {
        ignoringExceptions {
            TableClickhouse.drop(MergeTreeTable)
        }
        ignoringExceptions {
            TableClickhouse.drop(ReplacingMergeTreeTable)
        }
    }

    @Test
    fun createTableMergeTree_tableNotExists_tableSyncedWithDb() {
        TableClickhouse.create(MergeTreeTable)

        AssertDb.syncedWithDb(MergeTreeTable)

        TableClickhouse.drop(MergeTreeTable)
    }

    @Test
    fun createTableReplacingMergeTree_tableNotExists_tableSyncedWithDb() {
        TableClickhouse.create(ReplacingMergeTreeTable)

        AssertDb.syncedWithDb(ReplacingMergeTreeTable)

        TableClickhouse.drop(ReplacingMergeTreeTable)
    }
}

object MergeTreeTable: Table("MergeTreeTable", TestDatabase) {
    val date = date("date")
    val id = long("id").default { 1L }

    override val engine: Engine = MergeTree(date, listOf(id), 8192)
}

object ReplacingMergeTreeTable: Table("ReplacingMergeTreeTable", TestDatabase) {
    val date = date("date")
    val id = long("id").default { 1L }
    val version = ulong("version").default { 0L }

    override val engine: Engine = ReplacingMergeTree(date, listOf(id), version, 8192)
}