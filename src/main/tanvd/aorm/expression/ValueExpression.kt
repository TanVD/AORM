package tanvd.aorm.expression

import tanvd.aorm.DbType

class ValueExpression<E : Any, out T : DbType<E>>(val name: String, type: T) : Expression<E, T>(type) {
    override fun toSql(): String = name
}