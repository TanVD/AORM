package tanvd.aorm.query

import tanvd.aorm.DbType
import tanvd.aorm.expression.Expression

class Query(val from: String, var expressions: Set<Expression<*, DbType<*>>>) {
    var whereSection: QueryExpression? = null
    var prewhereSection: QueryExpression? = null
    var groupBySection: GroupByExpression? = null
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

infix fun Query.groupBy(expression: GroupByExpression): Query {
    groupBySection = expression
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