package tanvd.aorm.implementation

import tanvd.aorm.DbType
import tanvd.aorm.Row
import tanvd.aorm.Table
import tanvd.aorm.query.PreparedSqlResult
import tanvd.aorm.query.Query
import java.sql.Connection
import java.sql.PreparedStatement

object QueryClickhouse {
    fun getResult(table: Table, query: Query) : List<Row> {
        val rows = ArrayList<Row>()
        table.db.withConnection {
            constructQuery(query).use { statement ->
                val result = statement.executeQuery()
                while (result.next()) {
                    rows.add(Row(result, query.columns.map { it.resultColumn }))
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

    private fun Connection.constructQuery(query: Query) : PreparedStatement {
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
        var sql = "SELECT ${query.columns.joinToString { it.toSql() }} FROM ${query.table.name} "
        val valuesToSet = ArrayList<Pair<DbType<Any>, Any>>()
        if (query.prewhereSection != null) {
            val result = query.prewhereSection!!.toSqlPreparedDef()
            sql += "PREWHERE ${result.sql} "
            valuesToSet += result.data
        }
        if (query.whereSection != null) {
            val result = query.whereSection!!.toSqlPreparedDef()
            sql += "WHERE ${result.sql} "
            valuesToSet += result.data
        }
        if (query.orderBySection != null) {
            sql += "ORDER BY ${query.orderBySection!!.map.toList().joinToString { "${it.first.name} ${it.second}" }} "
        }
        if (query.limitSection != null) {
            sql += "LIMIT ${query.limitSection!!.offset} ${query.limitSection!!.limit} "
        }
        sql += ";"
        return PreparedSqlResult(sql, valuesToSet)
    }


}