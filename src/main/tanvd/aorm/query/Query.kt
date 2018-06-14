package tanvd.aorm.query

import tanvd.aorm.DbType
import tanvd.aorm.Table
import tanvd.aorm.expression.Expression

class Query(val table: Table, var columns: Set<Expression<*, DbType<*>>>) {
    var whereSection: QueryExpression? = null
    var prewhereSection: QueryExpression? = null
    var orderBySection: OrderByExpression? = null
    var limitSection: LimitExpression? = null

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