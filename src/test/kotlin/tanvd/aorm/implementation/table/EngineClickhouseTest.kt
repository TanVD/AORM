package tanvd.aorm.implementation.table

import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import tanvd.aorm.*
import tanvd.aorm.expression.default
import tanvd.aorm.implementation.TableClickhouse
import tanvd.aorm.utils.*

class EngineClickhouseTest : AormTestBase() {

    @BeforeEach
    fun resetTables() {
        tryRun {
            TableClickhouse.drop(database, MergeTreeTable)
        }
        tryRun {
            TableClickhouse.drop(database, ReplacingMergeTreeTable)
        }
        tryRun {
            TableClickhouse.drop(database, AggregatingMergeTreeTable)
        }
        tryRun {
            TableClickhouse.drop(database, CustomMergeTreeTable)
        }
    }

    @Test
    fun createTableMergeTree_tableNotExists_tableSyncedWithDb() {
        TableClickhouse.create(database, MergeTreeTable)

        AssertDb.syncedWithDb(database, MergeTreeTable)

        TableClickhouse.drop(database, MergeTreeTable)
    }

    @Test
    fun createTableReplacingMergeTree_tableNotExists_tableSyncedWithDb() {
        TableClickhouse.create(database, ReplacingMergeTreeTable)

        AssertDb.syncedWithDb(database, ReplacingMergeTreeTable)

        TableClickhouse.drop(database, ReplacingMergeTreeTable)
    }

    @Test
    fun createTableAggregatingMergeTree_tableNotExists_tableSyncedWithDb() {
        TableClickhouse.create(database, AggregatingMergeTreeTable)

        AssertDb.syncedWithDb(database, AggregatingMergeTreeTable)

        TableClickhouse.drop(database, AggregatingMergeTreeTable)
    }

    @Test
    fun createTableCustomMergeTree_tableNotExists_tableSyncedWithDb() {
        TableClickhouse.create(database, CustomMergeTreeTable)

        AssertDb.syncedWithDb(database, CustomMergeTreeTable)

        TableClickhouse.drop(database, CustomMergeTreeTable)
    }
}

object MergeTreeTable : Table("MergeTreeTable") {
    val date = date("date")
    val id = int64("id").default { 1L }

    override val engine: Engine = Engine.MergeTree(date, listOf(id), 8192)
}

object CustomMergeTreeTable : Table("CustomMergeTreeTable") {
    val date = date("date")
    val id = int64("id").default { 1L }

    override val engine: Engine.MergeTree = Engine.MergeTree(date, listOf(id), 8192).partitionBy(date).sampleBy(id).orderBy(id)
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
