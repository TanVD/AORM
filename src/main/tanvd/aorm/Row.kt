package tanvd.aorm

import tanvd.aorm.expression.Column
import tanvd.aorm.expression.Expression
import java.sql.ResultSet

data class Row(private val values: MutableMap<Expression<*, DbType<*>>, Any>) {
    val columns = LinkedHashSet(values.map { it.key })

    constructor(result: ResultSet, expressions: List<Expression<*, DbType<*>>>) : this(expressions.withIndex().map { (index, expr) ->
        expr to expr.getValue(result, index + 1)!!
    }.toMap().toMutableMap())

    constructor() : this(HashMap())

    operator fun <E : Any, K : DbType<E>> get(expression: Expression<E, K>): E? = values[expression] as? E?

    operator fun <E : Any, K : DbType<E>> set(key: Column<E, K>, value: E?) {
        value?.let {
            @Suppress("UNCHECKED_CAST")
            values[key] = value
            columns.add(key)
        }
    }
}