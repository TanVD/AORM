package tanvd.aorm

import tanvd.aorm.expression.Column
import java.util.*

sealed class Engine {
    abstract fun toSqlDef(): String

    class MergeTree(val dateColumn: Column<Date, DbPrimitiveType<Date>>, val primaryKey: List<Column<*, DbPrimitiveType<*>>>,
                    val indexGranularity: Long = 8192)
        : Engine() {
        override fun toSqlDef() = "MergeTree(${dateColumn.name}, (${primaryKey.joinToString { it.name }}), $indexGranularity)"
    }

    class ReplacingMergeTree(val dateColumn: Column<Date, DbPrimitiveType<Date>>, val primaryKey: List<Column<*, DbPrimitiveType<*>>>,
                             val versionColumn: Column<Long, DbUInt64>,
                             val indexGranularity: Long = 8192)
        : Engine() {
        override fun toSqlDef() = "ReplacingMergeTree(${dateColumn.name}, (${primaryKey.joinToString { it.name }})," +
                " $indexGranularity, ${versionColumn.name})"
    }
}