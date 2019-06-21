package tanvd.aorm.expression

import tanvd.aorm.*

//Count

class Count<E : Any, out T : DbType<E>>(val expression: Expression<E, T>) : Expression<Long, DbInt64>(DbInt64()) {
    override fun toSelectListDef(): String = "COUNT(${expression.toQueryQualifier()})"
    override fun toQueryQualifier(): String = "COUNT(${expression.toQueryQualifier()})"
}

fun <E : Any, T : DbType<E>> count(column: Column<E, T>): Count<E, T> = Count(column)

//ArgMax (State and Merge)
class ArgMaxState<E : Number, out T : DbNumericPrimitiveType<E>, Y : Any, Z : DbType<Y>>(val argExpression: Expression<E, T>, val valExpression: Expression<Y, Z>, type: Z) : Expression<Y, Z>(type) {
    override fun toSelectListDef(): String = "argMaxState(${argExpression.toQueryQualifier()}, ${valExpression.toQueryQualifier()})"
    override fun toQueryQualifier(): String = "argMaxState(${argExpression.toQueryQualifier()}, ${valExpression.toQueryQualifier()})"
}

fun <E : Number, T : DbNumericPrimitiveType<E>, Y : Any, Z : DbType<Y>> argMaxState(argExpression: Expression<E, T>,
                                                                                    valExpression: Expression<Y, Z>): ArgMaxState<E, T, Y, Z> = ArgMaxState(argExpression, valExpression, valExpression.type)

class ArgMaxMerge<E : Any, T : DbType<E>>(val expression: ArgMaxState<*, *, E, T>) : Expression<E, T>(expression.type) {
    override fun toSelectListDef(): String = "argMaxMerge(${expression.toQueryQualifier()})"
    override fun toQueryQualifier(): String = "argMaxMerge(${expression.toQueryQualifier()})"
}

class ArgMaxMergeMaterialized<E : Any, T : DbType<E>>(val expression: AliasedExpression<E, T, ArgMaxState<*, *, E, T>>) : Expression<E, T>(expression.type) {
    override fun toSelectListDef(): String = "argMaxMerge(${expression.toQueryQualifier()})"
    override fun toQueryQualifier(): String = "argMaxMerge(${expression.toQueryQualifier()})"
}

fun <E : Any, T : DbType<E>> argMaxMerge(expression: AliasedExpression<E, T, ArgMaxState<*, *, E, T>>): ArgMaxMergeMaterialized<E, T> = ArgMaxMergeMaterialized(expression)
fun <E : Any, T : DbType<E>> argMaxMerge(expression: ArgMaxState<*, *, E, T>): ArgMaxMerge<E, T> = ArgMaxMerge(expression)


//Max (State and Merge)
class MaxState<E : Number, out T : DbNumericPrimitiveType<E>>(val expression: Expression<E, T>, type: T) : Expression<E, T>(type) {
    override fun toSelectListDef(): String = "maxState(${expression.toQueryQualifier()})"
    override fun toQueryQualifier(): String = "maxState(${expression.toQueryQualifier()})"
}

fun <E : Number, T : DbNumericPrimitiveType<E>> maxState(column: Column<E, T>): MaxState<E, T> = MaxState(column, column.type)

class MaxMerge<E : Number, out T : DbNumericPrimitiveType<E>>(val expression: MaxState<E, T>) : Expression<E, T>(expression.type) {
    override fun toSelectListDef(): String = "maxMerge(${expression.toQueryQualifier()})"
    override fun toQueryQualifier(): String = "maxMerge(${expression.toQueryQualifier()})"
}

class MaxMergeMaterialized<E : Number, T : DbNumericPrimitiveType<E>>(val expression: AliasedExpression<E, T, MaxState<E, T>>) : Expression<E, T>(expression.type) {
    override fun toSelectListDef(): String = "maxMerge(${expression.toQueryQualifier()})"
    override fun toQueryQualifier(): String = "maxMerge(${expression.toQueryQualifier()})"
}

fun <E : Number, T : DbNumericPrimitiveType<E>> maxMerge(expression: AliasedExpression<E, T, MaxState<E, T>>): MaxMergeMaterialized<E, T> = MaxMergeMaterialized(expression)
fun <E : Number, T : DbNumericPrimitiveType<E>> maxMerge(expression: MaxState<E, T>): MaxMerge<E, T> = MaxMerge(expression)
