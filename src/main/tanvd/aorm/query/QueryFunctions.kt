package tanvd.aorm.query

import tanvd.aorm.Column
import tanvd.aorm.DbLong
import tanvd.aorm.DbType

abstract class QueryFunction(val innerColumn: Column<Any, DbType<Any>>, val resultColumn: Column<Any, DbType<Any>>) {
    abstract fun toSql(): String
}

class SimpleColumn<E: Any, out T: DbType<E>>(val column: Column<E, T>)
    : QueryFunction(column as Column<Any, DbType<Any>>, column as Column<Any, DbType<Any>>) {
    override fun toSql(): String {
        return column.name
    }
}

class Count<E: Any, out T: DbType<E>>(val column: Column<E, T>, val alias : String ):
        QueryFunction(column as Column<Any, DbType<Any>>, Column(alias, DbLong()) as Column<Any, DbType<Any>>){
    override fun toSql(): String {
        return "COUNT(${column.name}) as $alias"
    }
}

fun <E: Any, T: DbType<E>>count(column : Column<E, T>, alias: String): Count<E, T> {
    return Count(column, alias)
}