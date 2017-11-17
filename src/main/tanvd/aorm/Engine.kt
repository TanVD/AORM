package tanvd.aorm

import java.util.*


// Sealed class ?

sealed class Engine(val dateColumn: Column<Date, DbPrimitiveType<Date>>,
                     val primaryKey: List<Column<*, DbPrimitiveType<*>>>, val indexGranularity: Long) {
    abstract fun toSqlDef(): String

    class MergeTree(dateColumn: Column<Date, DbPrimitiveType<Date>>, primaryKey: List<Column<*, DbPrimitiveType<*>>>, indexGranularity: Long = 8192)
        : Engine(dateColumn, primaryKey, indexGranularity) {
        override fun toSqlDef() = "MergeTree(${dateColumn.name}, (${primaryKey.joinToString { it.name }}), $indexGranularity)"
    }

    class ReplacingMergeTree(dateColumn: Column<Date, DbPrimitiveType<Date>>, primaryKey: List<Column<*, DbPrimitiveType<*>>>,
                             val versionColumn: Column<Long, DbULong>,
                             indexGranularity: Long = 8192)
        : Engine(dateColumn, primaryKey, indexGranularity) {
        override fun toSqlDef() = "ReplacingMergeTree(${dateColumn.name}, (${primaryKey.joinToString { it.name }}), $indexGranularity, ${versionColumn.name})"
    }
}