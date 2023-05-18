package tanvd.aorm

import tanvd.aorm.expression.Column
import tanvd.aorm.expression.Expression
import java.sql.ResultSet

data class InsertRow(val values: MutableMap<Column<*, DbType<*>>, Any>) {
    val columns = LinkedHashSet(values.map { it.key })

    constructor() : this(HashMap())

    operator fun <E : Any, K : DbType<E>> set(key: Column<E, K>, value: E?) {
        if (value == null) {
            values.remove(key)
            columns.remove(key)
        } else {
            values[key] = value
            columns.add(key)
        }
    }

    @Suppress("UNCHECKED_CAST")
    operator fun <E : Any, K : DbType<E>> get(expression: Column<E, K>): E? = values[expression] as E?
}

data class SelectRow(val values: MutableMap<Expression<*, DbType<*>>, Any>) {
    val columns = LinkedHashSet(values.map { it.key })

    constructor(result: ResultSet, expressions: Set<Expression<*, DbType<*>>>) : this(
        expressions.withIndex().associateTo(hashMapOf()) { (index, expr) ->
            expr to expr.getValue(result, index + 1)
        })

    @Suppress("UNCHECKED_CAST")
    fun <E : Any, K : DbType<E>> getOrNull(expression: Expression<E, K>): E? = values[expression] as E?

    @Suppress("UNCHECKED_CAST")
    operator fun <E : Any, K : DbType<E>> get(expression: Expression<E, K>): E = values[expression] as E
}