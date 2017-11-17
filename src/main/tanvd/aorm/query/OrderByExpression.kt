package tanvd.aorm.query

import tanvd.aorm.Column
import tanvd.aorm.DbType

class OrderByExpression(val map: Map<Column<*, DbType<*>>, Order>)

enum class Order {
    ASC,
    DESC
}



//helper functions
fun Query.orderBy(vararg orderByMap: Pair<Column<*, DbType<*>>, Order>) : Query {
    return this orderBy OrderByExpression(orderByMap.toMap())
}

fun orderBy(vararg orderByMap: Pair<Column<*, DbType<*>>, Order>): OrderByExpression {
    return OrderByExpression(orderByMap.toMap())
}
