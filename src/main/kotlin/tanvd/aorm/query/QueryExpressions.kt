package tanvd.aorm.query

import tanvd.aorm.*
import tanvd.aorm.expression.*

sealed class QueryExpression {
    protected fun preparePlaceholderForType(type: DbType<*>?) : String {
        return if (type is DbDecimal) {
            "toDecimal${type.bit}(?, ${type.scale})"
        } else
            "?"
    }
    abstract fun toSqlPreparedDef(): PreparedSqlResult
}

/** CONTRACT: All sql's generated on every level should be in brackets. Inner sql should not be in brackets.**/

//Binary logic expressions
sealed class BinaryQueryExpression(val left: QueryExpression, val right: QueryExpression, val op: String) : QueryExpression() {
    override fun toSqlPreparedDef(): PreparedSqlResult {
        val resultLeft = left.toSqlPreparedDef()
        val resultRight = right.toSqlPreparedDef()
        return PreparedSqlResult("(${resultLeft.sql} $op ${resultRight.sql})", resultLeft.data + resultRight.data)
    }
}

class AndQueryExpression(left: QueryExpression, right: QueryExpression) : BinaryQueryExpression(left, right, "AND")
class OrQueryExpression(left: QueryExpression, right: QueryExpression) : BinaryQueryExpression(left, right, "OR")


//Unary logic expressions
sealed class UnaryQueryExpression(val expression: QueryExpression, val op: String) : QueryExpression() {
    override fun toSqlPreparedDef(): PreparedSqlResult {
        val result = expression.toSqlPreparedDef()
        return PreparedSqlResult("($op ${result.sql})", result.data)
    }
}

class NotQueryExpression(expression: QueryExpression) : UnaryQueryExpression(expression, "NOT")


//CONDITIONS

sealed class InfixConditionQueryExpression<E : Any, out T : DbType<E>, Y : Any>(val expression: Expression<E, T>, val value: Y,
                                                                                val type: DbType<Y>, val op: String) : QueryExpression() {
    override fun toSqlPreparedDef(): PreparedSqlResult {
        val paramPlaceholder = preparePlaceholderForType(type)
        @Suppress("UNCHECKED_CAST")
        return PreparedSqlResult("(${expression.toQueryQualifier()} $op $paramPlaceholder)", listOf((type to value) as Pair<DbType<Any>, Any>))
    }
}


sealed class PrefixConditionQueryExpression<E : Any, out T : DbType<E>>(val expression: Expression<E, T>,
                                                                        val op: String,
                                                                        val typeToValue: List<Pair<DbType<*>, *>>) : QueryExpression() {
    constructor(expression: Expression<E, T>, op: String, vararg typeToValue: Pair<DbType<*>, *>) : this(expression, op, typeToValue.toList())

    override fun toSqlPreparedDef(): PreparedSqlResult {
        val paramPlaceholder = preparePlaceholderForType(typeToValue.singleOrNull()?.first)
        @Suppress("UNCHECKED_CAST")
        return PreparedSqlResult("($op(${expression.toQueryQualifier()}, $paramPlaceholder))", typeToValue as List<Pair<DbType<Any>, Any>>)
    }
}

//VALUES

//Equality
class EqExpression<E : Any, out T : DbPrimitiveType<E>>(expression: Expression<E, T>, value: E) :
        InfixConditionQueryExpression<E, T, E>(expression, value, expression.type, "=")

//LessOrMore
class LessExpression<E : Any, out T : DbPrimitiveType<E>>(expression: Expression<E, T>, value: E) :
        InfixConditionQueryExpression<E, T, E>(expression, value, expression.type, "<")

class LessOrEqualExpression<E : Any, out T : DbPrimitiveType<E>>(expression: Expression<E, T>, value: E) :
        InfixConditionQueryExpression<E, T, E>(expression, value, expression.type, "<=")

class MoreExpression<E : Any, out T : DbPrimitiveType<E>>(expression: Expression<E, T>, value: E) :
        InfixConditionQueryExpression<E, T, E>(expression, value, expression.type, ">")

class MoreOrEqualExpression<E : Any, out T : DbPrimitiveType<E>>(expression: Expression<E, T>, value: E) :
        InfixConditionQueryExpression<E, T, E>(expression, value, expression.type, ">=")

class BetweenExpression<E : Any, out T : DbPrimitiveType<E>>(val expression: Expression<E, T>, val value: Pair<E, E>) : QueryExpression() {
    override fun toSqlPreparedDef(): PreparedSqlResult {
        @Suppress("UNCHECKED_CAST")
        return PreparedSqlResult("(${expression.toQueryQualifier()} between ? and ?)", listOf((expression.type to value.first) as Pair<DbType<Any>, Any>,
                (expression.type to value.second) as Pair<DbType<Any>, Any>))
    }
}

//Like
class LikeExpression(expression: Expression<String, DbPrimitiveType<String>>, value: String) :
        InfixConditionQueryExpression<String, DbPrimitiveType<String>, String>(expression, value, DbString(), "LIKE")

class RegexExpression(expression: Expression<String, DbPrimitiveType<String>>, value: String) :
        PrefixConditionQueryExpression<String, DbPrimitiveType<String>>(expression, "match", DbString() to value)

//Lists
class InListExpression<T : Any>(val expression: Expression<T, DbPrimitiveType<T>>, val value: List<T>) : QueryExpression() {
    @Suppress("UNCHECKED_CAST")
    override fun toSqlPreparedDef(): PreparedSqlResult {
        return if (value.isEmpty()) {
            PreparedSqlResult("(false)", emptyList())
        } else {
            PreparedSqlResult("(${expression.toQueryQualifier()} in (${value.joinToString { "?" }}))",
                    value.map { (expression.type to it) as Pair<DbType<Any>, Any> })
        }
    }
}


//ARRAYS
class HasExpression<T : Any>(expression: Expression<List<T>, DbArrayType<T>>, value: T)
    : PrefixConditionQueryExpression<List<T>, DbArrayType<T>>(expression, "has", expression.type.toPrimitive() to value)

class ExistsExpression<T : Any>(val expression: Expression<List<T>, DbArrayType<T>>,
                                val body: (Expression<T, DbPrimitiveType<T>>) -> QueryExpression) : QueryExpression() {
    override fun toSqlPreparedDef(): PreparedSqlResult {
        val innerColumn = ValueExpression("x", expression.type.toPrimitive())
        val innerExpression = body(innerColumn)
        val (sql, data) = innerExpression.toSqlPreparedDef()
        return PreparedSqlResult("(arrayExists(x -> $sql, ${expression.toQueryQualifier()}))", data)
    }
}


//To SQL class
data class PreparedSqlResult(val sql: String, val data: List<Pair<DbType<Any>, Any>>)


//Helper functions
//logic operators
infix fun QueryExpression.and(value: QueryExpression): AndQueryExpression = AndQueryExpression(this, value)

infix fun QueryExpression.or(value: QueryExpression): OrQueryExpression = OrQueryExpression(this, value)

fun not(value: QueryExpression): NotQueryExpression = NotQueryExpression(value)

//Equality
//Array equality is not supported by CH for now
infix fun <T : Any> Expression<T, DbPrimitiveType<T>>.eq(value: T): EqExpression<T, DbPrimitiveType<T>> =
        EqExpression(this, value)

//Numbers
infix fun <T : Any> Expression<T, DbPrimitiveType<T>>.less(value: T): LessExpression<T, DbPrimitiveType<T>> =
        LessExpression(this, value)

infix fun <T : Any> Expression<T, DbPrimitiveType<T>>.lessOrEq(value: T): LessOrEqualExpression<T, DbPrimitiveType<T>> =
        LessOrEqualExpression(this, value)

infix fun <T : Any> Expression<T, DbPrimitiveType<T>>.more(value: T): MoreExpression<T, DbPrimitiveType<T>> =
        MoreExpression(this, value)

infix fun <T : Any> Expression<T, DbPrimitiveType<T>>.moreOrEq(value: T): MoreOrEqualExpression<T, DbPrimitiveType<T>> =
        MoreOrEqualExpression(this, value)

infix fun <T : Any> Expression<T, DbPrimitiveType<T>>.between(value: Pair<T, T>): BetweenExpression<T, DbPrimitiveType<T>> =
        BetweenExpression(this, value)

//Strings
infix fun Expression<String, DbPrimitiveType<String>>.like(value: String): LikeExpression = LikeExpression(this, value)

infix fun Expression<String, DbPrimitiveType<String>>.regex(value: String): RegexExpression = RegexExpression(this, value)

//Lists
infix fun <T : Any> Expression<T, DbPrimitiveType<T>>.inList(values: List<T>): InListExpression<T> =
        InListExpression(this, values)


//Has arrays
infix fun <T : Any> Column<List<T>, DbArrayType<T>>.has(value: T): HasExpression<T> = HasExpression(this, value)

//Exists array
infix fun <T : Any> Column<List<T>, DbArrayType<T>>.exists(body: (Expression<T, DbPrimitiveType<T>>) -> QueryExpression): ExistsExpression<T> =
        ExistsExpression(this, body)

