package tanvd.aorm.expression

import tanvd.aorm.DbType

class ValueExpression<E : Any, out T : DbType<E>>(val name: String, type: T) : Expression<E, T>(type) {
    override fun toSelectListDef(): String = name

    override fun toQueryQualifier(): String = name
}