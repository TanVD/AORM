package tanvd.aorm.implementation

import tanvd.aorm.Database
import tanvd.aorm.DbType
import tanvd.aorm.SelectRow
import tanvd.aorm.query.PreparedSqlResult
import tanvd.aorm.query.Query
import java.sql.Connection
import java.sql.PreparedStatement

object QueryClickhouse {
    fun getResult(db: Database, query: Query): List<SelectRow> {
        val rows = ArrayList<SelectRow>()
        db.withConnection {
            constructQuery(query).use { statement ->
                val result = statement.executeQuery()
                while (result.next()) {
                    rows.add(SelectRow(result, query.columns))
                }
            }
        }
        return rows
    }

    fun constructQuery(query: Query): String {
        var (sql, valuesToSet) = preconstructQuery(query)
        valuesToSet.forEach { (type, value) ->
            sql = sql.replaceFirst("?", type.toStringValue(value))
        }
        return sql
    }

    private fun Connection.constructQuery(query: Query): PreparedStatement {
        val (sql, valuesToSet) = preconstructQuery(query)
        val statement = prepareStatement(sql)
        for ((index, pair) in valuesToSet.withIndex()) {
            val column = pair.first
            val value = pair.second
            column.setValue(index + 1, statement, value)
        }
        return statement
    }

    private fun preconstructQuery(query: Query): PreparedSqlResult {
        val valuesToSet = ArrayList<Pair<DbType<Any>, Any>>()
        val sql = buildString {
            append("SELECT ${query.columns.joinToString { it.toSql() }} FROM ${query.table.name} ")
            query.prewhereSection?.let { section ->
                val result = section.toSqlPreparedDef()
                append("PREWHERE ${result.sql} ")
                valuesToSet += result.data
            }
            query.whereSection?.let { section ->
                val result = section.toSqlPreparedDef()
                append("WHERE ${result.sql} ")
                valuesToSet += result.data
            }
            query.groupBySection?.let { section ->
                append("GROUP BY ${ section.columns.joinToString { it.name }} ")
            }
            query.orderBySection?.let { section ->
                append("ORDER BY ${section.map.toList().joinToString { "${it.first.name} ${it.second}" }} ")
            }
            query.limitSection?.let { section ->
                append("LIMIT ${section.offset}, ${query.limitSection!!.limit} ")
            }
            append(";")
        }
        return PreparedSqlResult(sql, valuesToSet)
    }


}