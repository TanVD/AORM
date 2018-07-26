package tanvd.aorm.query

import tanvd.aorm.DbType
import tanvd.aorm.expression.Column

class GroupByExpression(val columns: List<Column<*, DbType<*>>>)

//helper functions
fun Query.groupBy(vararg columns: Column<*, DbType<*>>): Query = this groupBy GroupByExpression(columns.toList())
