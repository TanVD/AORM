package tanvd.aorm

import tanvd.aorm.expression.Column
import tanvd.aorm.expression.Expression
import java.sql.ResultSet

data class Row<T : Expression<*, DbType<*>>>(val values: MutableMap<T, Any>) {
    val columns = LinkedHashSet(values.map { it.key })

    constructor(result: ResultSet, expressions: List<T>) : this(expressions.withIndex().map { (index, expr) ->
        expr to expr.getValue(result, index + 1)!!
    }.toMap().toMutableMap())

    constructor() : this(HashMap())

    @Suppress("UNCHECKED_CAST")
    operator fun <E : Any, Y : DbType<E>> get(column: Column<E, Y>): E? = values[column as? T] as? E?

    @Suppress("UNCHECKED_CAST")
    inline operator fun <E, K : DbType<E>> set(key: Column<E, K>, value: E) {
        values[key as T] = value as Any
        columns.add(key)
    }
}