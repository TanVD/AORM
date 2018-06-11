package tanvd.aorm

import tanvd.aorm.expression.Column
import tanvd.aorm.expression.Expression
import java.sql.ResultSet

data class InsertRow(val values: MutableMap<Column<Any, DbType<Any>>, Any>) {
    val columns = LinkedHashSet(values.map { it.key })

    constructor() : this(HashMap())

    @Suppress("UNCHECKED_CAST")
    operator fun <E : Any, K : DbType<E>> set(key: Column<E, K>, value: E?) {
        key as Column<Any, DbType<Any>>
        if (value == null) {
            values.remove(key)
            columns.remove(key)
        } else {
            values[key] = value
            columns.add(key)
        }
    }

    @Suppress("UNCHECKED_CAST")
    operator fun <E : Any, K : DbType<E>> get(expression: Column<E, K>): E? = values[expression as Column<Any, DbType<Any>>] as E?
}

data class SelectRow(val values: MutableMap<Expression<Any, DbType<Any>>, Any>) {
    val columns = LinkedHashSet(values.map { it.key })

    constructor(result: ResultSet, expressions: List<Expression<Any, DbType<Any>>>) : this(expressions.withIndex().map { (index, expr) ->
        expr to expr.getValue(result, index + 1)
    }.toMap().toMutableMap())

    @Suppress("UNCHECKED_CAST")
    operator fun <E : Any, K : DbType<E>> get(expression: Expression<E, K>): E? = values[expression as Expression<Any, DbType<Any>>] as E?
}