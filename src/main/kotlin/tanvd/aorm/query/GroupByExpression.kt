package tanvd.aorm.query

import tanvd.aorm.DbType
import tanvd.aorm.expression.Expression

class GroupByExpression(val columns: List<Expression<*, DbType<*>>>)

//helper functions
fun Query.groupBy(vararg columns: Expression<*, DbType<*>>): Query = this groupBy GroupByExpression(columns.toList())
