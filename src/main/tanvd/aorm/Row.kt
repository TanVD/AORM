package tanvd.aorm

import tanvd.aorm.expression.Column
import tanvd.aorm.expression.Expression
import java.sql.ResultSet

data class Row<T : Expression<*, DbType<*>>>(val values: Map<T, Any>) {
    val columns = values.map { it.key }

    constructor(result: ResultSet, expressions: List<T>) : this(expressions.withIndex().map { (index, expr) ->
        expr to expr.getValue(result, index + 1)!!
    }.toMap())

    @Suppress("UNCHECKED_CAST")
    operator fun <E : Any, Y : DbType<E>> get(column: Column<E, Y>): E? =
            values[column as? T] as? E?
}