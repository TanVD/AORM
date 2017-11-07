package tanvd.aorm

import java.sql.ResultSet

data class Row(val values: Map<Column<Any, DbType<Any>>, Any>) {
    val columns = values.map { it.key }

    constructor(result: ResultSet, columns: List<Column<Any, DbType<Any>>>) : this(columns.map { it to it.getValue(result) }.toMap())

    operator fun <E : Any, T: DbType<E>>get(column: Column<E, T>) : E? {
        return values[column as Column<Any, DbType<Any>>] as E?
    }

    operator fun get(name: String) : Any? {
        return values.filterKeys { it.name == name }.values.firstOrNull()
    }
}