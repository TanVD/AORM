package tanvd.aorm.expression

import tanvd.aorm.*
import tanvd.aorm.utils.escapeQuotes
import java.sql.PreparedStatement
import java.sql.ResultSet

abstract class Expression<E : Any, out T : DbType<E>>(val type: T) {
    abstract fun toQueryQualifier(): String
    abstract fun toSelectListDef(): String

    fun toStringValue(value: E): String = type.toStringValue(value)

    /** Index should start from 1.**/
    fun getValue(result: ResultSet, index: Int) = type.getValue(index, result)

    fun setValue(index: Int, statement: PreparedStatement, value: E) {
        type.setValue(index, statement, value)
    }


    /** Equals by query qualifier **/
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Expression<*, *>

        if (toQueryQualifier() != other.toQueryQualifier()) return false

        return true
    }

    override fun hashCode(): Int = toQueryQualifier().hashCode()
}

class AliasedExpression<E : Any, out T : DbType<E>, Y : Expression<E, T>>(val name: String, val expression: Y) :
        Expression<E, T>(expression.type) {
    var materializedInView: String? = null
    val alias = ValueExpression(name.escapeQuotes(), type)

    override fun toSelectListDef(): String = "${expression.toQueryQualifier()} as ${name.escapeQuotes()}"

    override fun toQueryQualifier(): String = name.escapeQuotes()
}

fun <E : Any, T : DbType<E>, Y : Expression<E, T>> alias(name: String, expression: Y): AliasedExpression<E, T, Y> = AliasedExpression(name, expression)
fun <E : Any, T : DbType<E>, Y : Expression<E, T>> View.alias(name: String, expression: Y): AliasedExpression<E, T, Y> = AliasedExpression(name, expression).also {
    it.materializedInView = this.name
}

fun <E : Any, T : DbType<E>, Y : Expression<E, T>> MaterializedView.alias(name: String, expression: Y): AliasedExpression<E, T, Y> = AliasedExpression(name, expression).also {
    it.materializedInView = this.name
}
