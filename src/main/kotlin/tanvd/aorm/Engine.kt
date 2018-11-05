package tanvd.aorm

import tanvd.aorm.expression.Column
import tanvd.aorm.expression.Expression
import java.util.*

sealed class Engine {
    abstract fun toSqlDef(): String

    abstract fun toReplicatedSqlDef(index: Int): String

    class MergeTree(val dateColumn: Expression<Date, DbPrimitiveType<Date>>, val primaryKey: List<Expression<*, DbPrimitiveType<*>>>,
                    val indexGranularity: Long = 8192, val zookeeperPath: String? = null)
        : Engine() {
        override fun toSqlDef() = "MergeTree(${dateColumn.toQueryQualifier()}, (${primaryKey.joinToString { it.toQueryQualifier() }}), $indexGranularity)"

        override fun toReplicatedSqlDef(index: Int): String = zookeeperPath?.let { zookeeperPath ->
            "ReplicatedMergeTree($zookeeperPath, $index, ${dateColumn.toQueryQualifier()}, (${primaryKey.joinToString { it.toQueryQualifier() }}), $indexGranularity)"
        } ?: throw NotImplementedError("Zookeeper path not stated. Replicated table can not be created.")

    }

    class MergeTreeCustom(
            val partitionBy: Array<String>,
            val orderBy: Array<String>,
            val sampleBy: Array<String>? = null,
            val zookeeperPath: String? = null
    ): Engine() {

        private fun combineProperties(): String =
                "PARTITION BY (${partitionBy.joinToString(",")}) ORDER BY (${orderBy.joinToString(",")})"
                        .let {
                            if (sampleBy != null && sampleBy.isNotEmpty()) {
                                 it + " SAMPLE BY (${sampleBy.joinToString(",")})"
                            } else
                                it
                        }

        override fun toSqlDef(): String = "MergeTree() " + combineProperties()

        override fun toReplicatedSqlDef(index: Int): String = zookeeperPath?.let { zookeeperPath ->
            "ReplicatedMergeTree($zookeeperPath) " + combineProperties()
        } ?: throw NotImplementedError("Zookeeper path not stated. Replicated table can not be created.")
    }

    class ReplacingMergeTree(val dateColumn: Expression<Date, DbPrimitiveType<Date>>, val primaryKey: List<Expression<*, DbPrimitiveType<*>>>,
                             val versionColumn: Column<Long, DbUInt64>,
                             val indexGranularity: Long = 8192,
                             val zookeeperPath: String? = null)
        : Engine() {
        override fun toSqlDef() = "ReplacingMergeTree(${dateColumn.toQueryQualifier()}, (${primaryKey.joinToString { it.toQueryQualifier() }})," +
                " $indexGranularity, ${versionColumn.name})"

        override fun toReplicatedSqlDef(index: Int): String = zookeeperPath?.let { zookeeperPath ->
            "ReplicatedReplacingMergeTree($zookeeperPath, $index, ${dateColumn.toQueryQualifier()}, (${primaryKey.joinToString { it.toQueryQualifier() }}), $indexGranularity, ${versionColumn.name})"
        } ?: throw NotImplementedError("Zookeeper path not stated. Replicated table can not be created.")
    }

    class AggregatingMergeTree(val dateColumn: Expression<Date, DbPrimitiveType<Date>>, val primaryKey: List<Expression<*, DbPrimitiveType<*>>>,
                               val indexGranularity: Long = 8192, val zookeeperPath: String? = null)
        : Engine() {
        override fun toSqlDef() = "AggregatingMergeTree(${dateColumn.toQueryQualifier()}, (${primaryKey.joinToString { it.toQueryQualifier() }}), $indexGranularity)"

        override fun toReplicatedSqlDef(index: Int): String = zookeeperPath?.let { zookeeperPath ->
            "ReplicatedAggregatingMergeTree($zookeeperPath, $index, ${dateColumn.toQueryQualifier()}, (${primaryKey.joinToString { it.toQueryQualifier() }}), $indexGranularity)"
        } ?: throw NotImplementedError("Zookeeper path not stated. Replicated table can not be created.")
    }
}