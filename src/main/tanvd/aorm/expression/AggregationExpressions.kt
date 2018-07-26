package tanvd.aorm.expression

import tanvd.aorm.DbIntPrimitiveType

//Max (State and Merge)
class MaxState<E : Number, out T : DbIntPrimitiveType<E>>(val expression: Expression<E, T>, type: T) : Expression<E, T>(type) {
    override fun toSql(): String = "maxState(${expression.toSql()})"
}
fun <E : Number, T : DbIntPrimitiveType<E>> maxState(column: Column<E, T>): MaxState<E, T> = MaxState(column, column.type)


class MaxMerge<E : Number, out T : DbIntPrimitiveType<E>>(val expression: MaxState <E, T>) : Expression<E, T>(expression.type) {
    override fun toSql(): String = "maxMerge(${expression.toSql()})"
}

class MaxMergeMaterialized<E : Number, T : DbIntPrimitiveType<E>>(val expression: MaterializedExpression<E, T, MaxState<E, T>>) : Expression<E, T>(expression.type) {
    override fun toSql(): String = "maxMerge(${expression.toSql()})"
}

fun <E : Number, T : DbIntPrimitiveType<E>> maxMerge(expression: MaterializedExpression<E, T, MaxState<E, T>>): MaxMergeMaterialized<E, T> = MaxMergeMaterialized(expression)

fun <E : Number, T : DbIntPrimitiveType<E>> maxMerge(expression: MaxState<E, T>): MaxMerge<E, T> = MaxMerge(expression)