package tanvd.aorm.expression

import tanvd.aorm.DbType
import tanvd.aorm.utils.escapeQuotes

class ValueExpression<E : Any, out T : DbType<E>>(val name: String, type: T) : Expression<E, T>(type) {
    override fun toSelectListDef(): String = name.escapeQuotes()

    override fun toQueryQualifier(): String = name.escapeQuotes()
}

class SqlValueExpression<E : Any, out T : DbType<E>>(val value: String, type: T) : Expression<E, T>(type) {
    override fun toSelectListDef(): String = value

    override fun toQueryQualifier(): String = value
}

fun <E : Any, T : DbType<E>> sqlValue(value: String, type: T): SqlValueExpression<E, T> = SqlValueExpression(value, type)