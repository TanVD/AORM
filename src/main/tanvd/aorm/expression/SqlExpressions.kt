package tanvd.aorm.expression

import tanvd.aorm.DbInt64
import tanvd.aorm.DbIntPrimitiveType
import tanvd.aorm.DbType

class Count<E : Any, out T : DbType<E>>(val expression: Expression<E, T>) : Expression<Long, DbInt64>(DbInt64()) {
    override fun toSql(): String = "COUNT(${expression.toSql()})"
}

fun <E : Any, T : DbType<E>> count(column: Column<E, T>): Count<E, T> = Count(column)

class MaxMerge<E : Number, out T : DbIntPrimitiveType<E>>(val expression: Expression<E, T>, type: T) : Expression<E, T>(type) {
    override fun toSql(): String = "maxMerge(${expression.toSql()})"
}

fun <E : Number, T : DbIntPrimitiveType<E>> maxMerge(column: Column<E, T>): MaxMerge<E, T> = MaxMerge(column, column.type)