package tanvd.aorm.implementation.table

import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.aorm.Engine
import tanvd.aorm.Table
import tanvd.aorm.expression.default
import tanvd.aorm.implementation.TableClickhouse
import tanvd.aorm.utils.AssertDb
import tanvd.aorm.utils.TestDatabase
import tanvd.aorm.utils.ignoringExceptions

class EngineClickhouseTest {

    @BeforeMethod
    fun resetTables() {
        ignoringExceptions {
            TableClickhouse.drop(TestDatabase, MergeTreeTable)
        }
        ignoringExceptions {
            TableClickhouse.drop(TestDatabase, ReplacingMergeTreeTable)
        }
    }

    @Test
    fun createTableMergeTree_tableNotExists_tableSyncedWithDb() {
        TableClickhouse.create(TestDatabase, MergeTreeTable)

        AssertDb.syncedWithDb(MergeTreeTable)

        TableClickhouse.drop(TestDatabase, MergeTreeTable)
    }

    @Test
    fun createTableReplacingMergeTree_tableNotExists_tableSyncedWithDb() {
        TableClickhouse.create(TestDatabase, ReplacingMergeTreeTable)

        AssertDb.syncedWithDb(ReplacingMergeTreeTable)

        TableClickhouse.drop(TestDatabase, ReplacingMergeTreeTable)
    }

    @Test
    fun createTableAggregatingMergeTree_tableNotExists_tableSyncedWithDb() {
        TableClickhouse.create(TestDatabase, AggregatingMergeTreeTable)

        AssertDb.syncedWithDb(AggregatingMergeTreeTable)

        TableClickhouse.drop(TestDatabase, AggregatingMergeTreeTable)
    }
}

object MergeTreeTable : Table("MergeTreeTable") {
    val date = date("date")
    val id = int64("id").default { 1L }

    override val engine: Engine = Engine.MergeTree(date, listOf(id), 8192)
}

object ReplacingMergeTreeTable : Table("ReplacingMergeTreeTable") {
    val date = date("date")
    val id = int64("id").default { 1L }
    val version = uint64("version").default { 0L }

    override val engine: Engine = Engine.ReplacingMergeTree(date, listOf(id), version, 8192)
}

object AggregatingMergeTreeTable : Table("AggregatingMergeTreeTable") {
    val date = date("date")
    val id = int64("id").default { 1L }

    override val engine: Engine = Engine.AggregatingMergeTree(date, listOf(id))
}