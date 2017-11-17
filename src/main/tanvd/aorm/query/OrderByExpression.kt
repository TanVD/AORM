package tanvd.aorm.query

import tanvd.aorm.DbType
import tanvd.aorm.expression.Column

class OrderByExpression(val map: Map<Column<*, DbType<*>>, Order>)

enum class Order {
    ASC,
    DESC
}

//helper functions
fun Query.orderBy(vararg orderByMap: Pair<Column<*, DbType<*>>, Order>): Query =
        this orderBy OrderByExpression(orderByMap.toMap())
