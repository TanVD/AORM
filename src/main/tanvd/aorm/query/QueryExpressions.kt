package tanvd.aorm.query

import tanvd.aorm.*

sealed class QueryExpression {
    abstract fun toSqlPreparedDef() : PreparedSqlResult
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

class NotQueryExpression(expression: QueryExpression): UnaryQueryExpression(expression, "NOT")


//CONDITIONS

sealed class InfixConditionQueryExpression<E: Any, out T : DbType<E>, Y : Any>(val column: Column<E, T>, val value: Y,
                                                                               val type: DbType<Y>, val op: String) : QueryExpression() {
    override fun toSqlPreparedDef(): PreparedSqlResult {
        @Suppress("UNCHECKED_CAST")
        return PreparedSqlResult("(${column.name} $op ?)", listOf((type to value) as Pair<DbType<Any>, Any>))
    }
}

sealed class PrefixConditionQueryExpression<E: Any, out T : DbType<E>, Y : Any>(val column: Column<E, T>, val value: Y,
                                                                                val type: DbType<Y>, val op: String) : QueryExpression() {
    override fun toSqlPreparedDef(): PreparedSqlResult {
        @Suppress("UNCHECKED_CAST")
        return PreparedSqlResult("($op(${column.name}, ?))", listOf((type to value) as Pair<DbType<Any>, Any>))
    }
}

//VALUES

//Equality
class EqExpression<E : Any, out T : DbPrimitiveType<E>>(column: Column<E, T>, value: E) :
        InfixConditionQueryExpression<E, T, E>(column, value, column.type, "=")

//LessOrMore
class LessExpression<E : Any, out T : DbPrimitiveType<E>>(column: Column<E, T>, value: E) :
        InfixConditionQueryExpression<E, T, E>(column, value, column.type, "<")

class LessOrEqualExpression<E : Any, out T : DbPrimitiveType<E>>(column: Column<E, T>, value: E) :
        InfixConditionQueryExpression<E, T, E>(column, value, column.type, "<=")

class MoreExpression<E : Any, out T : DbPrimitiveType<E>>(column: Column<E, T>, value: E) :
        InfixConditionQueryExpression<E, T, E>(column, value, column.type, ">")

class MoreOrEqualExpression<E : Any, out T : DbPrimitiveType<E>>(column: Column<E, T>, value: E) :
        InfixConditionQueryExpression<E, T, E>(column, value, column.type, ">=")

//Like
class LikeExpression(column: Column<String, DbPrimitiveType<String>>, value: String) :
        InfixConditionQueryExpression<String, DbPrimitiveType<String>, String>(column, value, DbString(), "LIKE")

class RegexExpression(column: Column<String, DbPrimitiveType<String>>, value: String) :
        PrefixConditionQueryExpression<String, DbPrimitiveType<String>, String>(column, value, DbString(), "match")

//Lists
class InListExpression<T: Any>(val column: Column<T, DbPrimitiveType<T>>, val value: List<T>) : QueryExpression() {
    override fun toSqlPreparedDef(): PreparedSqlResult {
        return PreparedSqlResult("(${column.name} in (${value.joinToString { "?" }}))",
                value.map { (column.type to it) as Pair<DbType<Any>, Any> })
    }
}


//ARRAYS
class HasExpression<T: Any>(column: Column<List<T>, DbArrayType<T>>, value: T)
    : PrefixConditionQueryExpression<List<T>, DbArrayType<T>, T>(column, value, column.type.toPrimitive(), "has")

class ExistsExpression<T: Any>(val column: Column<List<T>, DbArrayType<T>>,
                               val body: (Column<T, DbPrimitiveType<T>>) -> QueryExpression) : QueryExpression() {
    override fun toSqlPreparedDef(): PreparedSqlResult {
        val innerColumn = Column("x", column.type.toPrimitive())
        val innerExpression = body(innerColumn)
        val (sql, data) = innerExpression.toSqlPreparedDef()
        return PreparedSqlResult("(arrayExists(x -> $sql, ${column.name}))", data)
    }
}


//To SQL class
data class PreparedSqlResult(val sql: String, val data: List<Pair<DbType<Any>, Any>>)


//Helper functions
//logic operators
infix fun QueryExpression.and(value: QueryExpression): AndQueryExpression {
    return AndQueryExpression(this, value)
}

infix fun QueryExpression.or(value: QueryExpression): OrQueryExpression {
    return OrQueryExpression(this, value)
}

fun not(value: QueryExpression): NotQueryExpression {
    return NotQueryExpression(value)
}

//Equality
//Array equality is not supported by CH for now
infix fun <T: Any> Column<T, DbPrimitiveType<T>>.eq(value: T) : EqExpression<T, DbPrimitiveType<T>> {
    return EqExpression(this, value)
}

//Numbers
infix fun <T : Any> Column<T, DbPrimitiveType<T>>.less(value: T) : LessExpression<T, DbPrimitiveType<T>> {
    return LessExpression(this, value)
}

infix fun <T : Any> Column<T, DbPrimitiveType<T>>.lessOrEq(value: T) : LessOrEqualExpression<T, DbPrimitiveType<T>> {
    return LessOrEqualExpression(this, value)
}

infix fun <T : Any> Column<T, DbPrimitiveType<T>>.more(value: T) : MoreExpression<T, DbPrimitiveType<T>> {
    return MoreExpression(this, value)
}

infix fun <T : Any> Column<T, DbPrimitiveType<T>>.moreOrEq(value: T) : MoreOrEqualExpression<T, DbPrimitiveType<T>> {
    return MoreOrEqualExpression(this, value)
}

//Strings
infix fun Column<String, DbPrimitiveType<String>>.like(value: String): LikeExpression {
    return LikeExpression(this, value)
}

infix fun Column<String, DbPrimitiveType<String>>.regex(value: String): RegexExpression {
    return RegexExpression(this, value)
}

//Lists
infix fun <T: Any>Column<T, DbPrimitiveType<T>>.inList(values: List<T>): InListExpression<T> {
    return InListExpression(this, values)
}


//Has arrays
infix fun <T: Any>Column<List<T>, DbArrayType<T>>.has(value: T): HasExpression<T> {
    return HasExpression(this, value)
}

//Exists array
infix fun <T: Any>Column<List<T>, DbArrayType<T>>.exists(body: (Column<T, DbPrimitiveType<T>>) -> QueryExpression): ExistsExpression<T> {
    return ExistsExpression(this, body)
}

