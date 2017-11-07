package tanvd.aorm

import java.sql.PreparedStatement
import java.sql.ResultSet

class Column<E, out T : DbType<E>>(val name: String, val type: T, default: (() -> E)? = null) {
    var defaultFunction: (() -> E)? = default

    fun getValue(result: ResultSet) : E {
        return type.getValue(name, result)
    }

    fun setValue(index: Int, statement: PreparedStatement, value: E) {
        type.setValue(index, statement, value)
    }

    fun toStringValue(value: E): String {
        return type.toStringValue(value)
    }

    fun toSqlDef(): String {
        return "$name ${type.toSqlName()}"
    }

    //Equals by name
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Column<*, *>

        if (name != other.name) return false

        return true
    }

    override fun hashCode(): Int {
        return name.hashCode()
    }
}


//Helper function
infix fun <E: Any, T : DbType<E>> Column<E, T>.default(func: () -> E) : Column<E, T> {
    defaultFunction = func
    return this
}

