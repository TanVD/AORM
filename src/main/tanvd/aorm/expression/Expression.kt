package tanvd.aorm.expression

import tanvd.aorm.DbType
import java.sql.PreparedStatement
import java.sql.ResultSet

abstract class Expression<E: Any, out T : DbType<E>>(val type: T) {
    abstract fun toQueryQualifier(): String
    abstract fun toSelectListDef(): String

    fun toStringValue(value: E): String = type.toStringValue(value)

    /** Index should start from 1.**/
    fun getValue(result: ResultSet, index: Int) = type.getValue(index, result)

    fun setValue(index: Int, statement: PreparedStatement, value: E) {
        type.setValue(index, statement, value)
    }
}

class AliasedExpression<E: Any, out T : DbType<E>, Y: Expression<E, T>>(val name: String, val expression: Y):
        Expression<E, T>(expression.type) {
    val alias = ValueExpression(name, type)

    override fun toSelectListDef(): String = "${expression.toQueryQualifier()} as $name"

    override fun toQueryQualifier(): String = name
}

fun <E: Any, T : DbType<E>, Y: Expression<E, T>> alias(name: String, expression: Y): AliasedExpression<E, T, Y> {
    return AliasedExpression(name, expression)
}