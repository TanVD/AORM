package tanvd.aorm.query

import tanvd.aorm.DbType
import tanvd.aorm.expression.Column
import tanvd.aorm.expression.Expression

class OrderByExpression(val map: Map<Expression<*, DbType<*>>, Order>)

enum class Order {
    ASC,
    DESC
}

//helper functions
fun Query.orderBy(vararg orderByMap: Pair<Expression<*, DbType<*>>, Order>): Query =
        this orderBy OrderByExpression(orderByMap.toMap())
