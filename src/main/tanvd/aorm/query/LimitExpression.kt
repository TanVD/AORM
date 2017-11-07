package tanvd.aorm.query

class LimitExpression(val limit: Long, val offset: Long)



//helper functions
infix fun Query.limit(limitAndOffset: Pair<Long, Long>) : Query {
    return this limit LimitExpression(limitAndOffset.first, limitAndOffset.second)
}

fun limit(limit: Long, offset: Long) : LimitExpression {
    return LimitExpression(limit, offset)
}
