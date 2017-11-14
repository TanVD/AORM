package tanvd.aorm.query

class LimitExpression(val limit: Long, val offset: Long)



//helper functions
fun Query.limit(limit: Long, offset: Long) : Query {
    return this limit LimitExpression(limit, offset)
}

fun limit(limit: Long, offset: Long): LimitExpression {
    return LimitExpression(limit, offset)
}
