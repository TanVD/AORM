package tanvd.aorm

import tanvd.aorm.expression.Column
import tanvd.aorm.expression.Expression
import java.math.BigInteger
import java.sql.Date

sealed class Engine {
    abstract fun toSqlDef(): String

    abstract fun toReplicatedSqlDef(index: Int): String

    abstract class MergeTreeFamily(private val dateColumn: Expression<Date, DbPrimitiveType<Date>>,
                                   private val primaryKey: List<Expression<*, DbPrimitiveType<*>>>,
                                   private val indexGranularity: Long = 8192,
                                   private val zookeeperPath: String? = null,
                                   internal var partition_by: List<Expression<*, DbPrimitiveType<*>>>? = null,
                                   internal var order_by: List<Expression<*, DbPrimitiveType<*>>>? = null,
                                   internal var sample_by: Expression<*, DbPrimitiveType<*>>? = null) : Engine() {

        open val familyModifier: String = ""

        open val specificParams: String? = null

        val extendedSyntaxUsed: Boolean
            get() = partition_by != null || order_by != null || sample_by != null

        private val mainSyntaxDef: String
            get() = "${dateColumn.toQueryQualifier()}, (${primaryKey.joinToString { it.toQueryQualifier() }}), $indexGranularity"

        private val extendedSyntaxDef: String
            get() = (partition_by?.let { "PARTITION BY (${it.joinToString { it.toQueryQualifier() }}) " } ?: "") +
                    (order_by?.let { "ORDER BY (${it.joinToString { it.toQueryQualifier() }}) " } ?: "") +
                    (sample_by?.let { "SAMPLE BY ${it.toQueryQualifier()} " } ?: "")

        override fun toSqlDef() = if (extendedSyntaxUsed)
            "${familyModifier}MergeTree(${specificParams ?: ""}) $extendedSyntaxDef"
        else
            "${familyModifier}MergeTree($mainSyntaxDef${specificParams?.let { ", $it" } ?: ""})"


        override fun toReplicatedSqlDef(index: Int): String = zookeeperPath?.let { zookeeperPath ->
            if (extendedSyntaxUsed)
                "Replicated${familyModifier}MergeTree($zookeeperPath, $index${specificParams?.let { ", $it" } ?: ""}) $extendedSyntaxDef"
            else
                "Replicated${familyModifier}MergeTree($zookeeperPath, $index, $mainSyntaxDef${specificParams?.let { ", $it" } ?: ""})"
        } ?: throw NotImplementedError("Zookeeper path not stated. Replicated table can not be created.")
    }

    class MergeTree(dateColumn: Expression<Date, DbPrimitiveType<Date>>,
                    primaryKey: List<Expression<*, DbPrimitiveType<*>>>,
                    indexGranularity: Long = 8192,
                    zookeeperPath: String? = null) : MergeTreeFamily(dateColumn, primaryKey, indexGranularity, zookeeperPath)

    class ReplacingMergeTree(dateColumn: Expression<Date, DbPrimitiveType<Date>>,
                             primaryKey: List<Expression<*, DbPrimitiveType<*>>>,
                             versionColumn: Column<BigInteger, DbUInt64>,
                             indexGranularity: Long = 8192,
                             zookeeperPath: String? = null)
        : MergeTreeFamily(dateColumn, primaryKey, indexGranularity, zookeeperPath) {
        override val familyModifier = "Replacing"

        override val specificParams = versionColumn.name
    }

    class AggregatingMergeTree(dateColumn: Expression<Date, DbPrimitiveType<Date>>,
                               primaryKey: List<Expression<*, DbPrimitiveType<*>>>,
                               indexGranularity: Long = 8192,
                               zookeeperPath: String? = null) : MergeTreeFamily(dateColumn, primaryKey, indexGranularity, zookeeperPath) {
        override val familyModifier = "Aggregating"
    }
}

fun <T : Engine.MergeTreeFamily> T.partitionBy(vararg key: Expression<*, DbPrimitiveType<*>>) = this.also {
    it.partition_by = key.toList()
}

fun <T : Engine.MergeTreeFamily> T.orderBy(vararg key: Expression<*, DbPrimitiveType<*>>) = this.also {
    it.order_by = key.toList()
}

fun <T : Engine.MergeTreeFamily> T.sampleBy(key: Expression<*, DbPrimitiveType<*>>) = this.also {
    it.sample_by = key
}
