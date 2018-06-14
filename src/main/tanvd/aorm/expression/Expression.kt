package tanvd.aorm.expression

import tanvd.aorm.DbType
import java.sql.PreparedStatement
import java.sql.ResultSet

abstract class Expression<E: Any, out T : DbType<E>>(val type: T) {
    abstract fun toSql(): String

    fun toStringValue(value: E): String = type.toStringValue(value)

    /** Index should start from 1.**/
    fun getValue(result: ResultSet, index: Int) = type.getValue(index, result)

    fun setValue(index: Int, statement: PreparedStatement, value: E) {
        type.setValue(index, statement, value)
    }
}