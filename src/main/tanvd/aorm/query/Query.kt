package tanvd.aorm.query

import tanvd.aorm.DbType
import tanvd.aorm.Row
import tanvd.aorm.Table
import tanvd.aorm.expression.Expression
import tanvd.aorm.implementation.QueryClickhouse

class Query(val table: Table, var columns: List<Expression<Any, DbType<Any>>>) {
    var whereSection: QueryExpression? = null
    var prewhereSection: QueryExpression? = null
    var orderBySection: OrderByExpression? = null
    var limitSection: LimitExpression? = null

    fun toResult(): List<Row<Expression<Any, DbType<Any>>>> = QueryClickhouse.getResult(this)
}

//Helper functions
infix fun Query.where(expression: QueryExpression): Query {
    whereSection = expression
    return this
}

infix fun Query.prewhere(expression: QueryExpression): Query {
    prewhereSection = expression
    return this
}

infix fun Query.orderBy(expression: OrderByExpression): Query {
    orderBySection = expression
    return this
}

infix fun Query.limit(expression: LimitExpression): Query {
    limitSection = expression
    return this
}