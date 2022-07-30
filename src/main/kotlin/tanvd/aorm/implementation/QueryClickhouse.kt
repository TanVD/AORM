package tanvd.aorm.implementation

import tanvd.aorm.*
import tanvd.aorm.expression.AliasedExpression
import tanvd.aorm.expression.Expression
import tanvd.aorm.query.PreparedSqlResult
import tanvd.aorm.query.Query
import tanvd.aorm.utils.use

object QueryClickhouse {

    fun getResult(db: Database, query: Query): List<SelectRow> {
        val preparedQuery = preconstructQuery(query)
        return getResult(db, preparedQuery, query.expressions)
    }

    /**
     * Low level plain text SQL queries execution support
     */
    fun getResult(db: Database, preparedQuery: PreparedSqlResult, expressions: Set<Expression<*, DbType<*>>>): List<SelectRow> {
        val rows = ArrayList<SelectRow>()
        db.withConnection {
            val statement = prepareStatement(preparedQuery.sql)
            for ((index, pair) in preparedQuery.data.withIndex()) {
                val column = pair.first
                val value = pair.second
                column.setValue(index + 1, statement, value)
            }
            statement.use {
                val result = statement.executeQuery()
                while (result.next()) {
                    rows.add(SelectRow(result, expressions))
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

    private fun preconstructQuery(query: Query): PreparedSqlResult {
        val valuesToSet = ArrayList<Pair<DbType<Any>, Any>>()
        val sql = buildString {
            append("SELECT ")
            if (query.withDistinct)
                append("DISTINCT ")
            query.expressions.joinTo(this) {
                if (query.from == (it as? AliasedExpression<*, *, *>)?.materializedInView) {
                    it.toQueryQualifier()
                } else {
                    it.toSelectListDef()
                }
            }
            append(" FROM ${query.from}")
            query.prewhereSection?.let { section ->
                val result = section.toSqlPreparedDef()
                append(" PREWHERE ${result.sql}")
                valuesToSet += result.data
            }
            query.whereSection?.let { section ->
                val result = section.toSqlPreparedDef()
                append(" WHERE ${result.sql}")
                valuesToSet += result.data
            }
            query.groupBySection?.let { section ->
                append(" GROUP BY ${section.columns.joinToString { it.toQueryQualifier() }}")
            }
            query.orderBySection?.let { section ->
                append(" ORDER BY ${section.map.toList().joinToString { "${it.first.toQueryQualifier()} ${it.second}" }}")
            }
            query.limitSection?.let { section ->
                append(" LIMIT ${section.offset}, ${section.limit}")
            }
            query.settings?.let { section ->
                append(" SETTINGS ${section.settings.joinToString { "${it.first} = ${it.second}" }}")
            }
        }
        return PreparedSqlResult(sql, valuesToSet)
    }
}
