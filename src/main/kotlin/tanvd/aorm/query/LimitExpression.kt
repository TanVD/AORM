package tanvd.aorm.query

class LimitExpression(val limit: Long, val offset: Long)


//helper functions
fun Query.limit(limit: Long, offset: Long): Query = this limit LimitExpression(limit, offset)