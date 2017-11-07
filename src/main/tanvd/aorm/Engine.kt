package tanvd.aorm

import java.util.*

interface Engine {
    fun toSqlDef(): String
}

class MergeTree(val dateColumn: Column<Date, DbPrimitiveType<Date>>,
                val primaryKey: List<Column<*, DbPrimitiveType<*>>>, val indexGranularity: Long = 8192) : Engine {
    override fun toSqlDef(): String {
        return "MergeTree(${dateColumn.name}, (${primaryKey.joinToString { it.name }}), $indexGranularity)"
    }
}

class ReplacingMergeTree(val dateColumn: Column<Date, DbPrimitiveType<Date>>,
                         val primaryKey: List<Column<*, DbPrimitiveType<*>>>,
                         val versionColumn: Column<Long, DbULong>, val indexGranularity: Long = 8192) : Engine {
    override fun toSqlDef(): String {
        return "ReplacingMergeTree(${dateColumn.name}, (${primaryKey.joinToString { it.name }}), ${versionColumn.name}," +
                " $indexGranularity)"
    }
}