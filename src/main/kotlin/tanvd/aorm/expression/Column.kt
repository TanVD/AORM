package tanvd.aorm.expression

import tanvd.aorm.DbType
import tanvd.aorm.Table
import tanvd.aorm.utils.escapeQuotes

class Column<E : Any, out T : DbType<E>>(val name: String, type: T, val table: Table,
                                         default: (() -> E)? = null) : Expression<E, T>(type) {
    override fun toQueryQualifier(): String = name.escapeQuotes()

    override fun toSelectListDef(): String = name.escapeQuotes()

    fun toSqlDef(): String = "${name.escapeQuotes()} ${type.toSqlName()}"

    var defaultFunction: (() -> E)? = default

    internal fun defaultValueResolved(): E = defaultFunction?.invoke() ?: type.defaultValue

    /** Equals by name and table **/
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Column<*, *>

        if (name != other.name) return false
        if (table != other.table) return false

        return true
    }

    override fun hashCode(): Int {
        var result = name.hashCode()
        result = 31 * result + table.hashCode()
        return result
    }
}


//Helper function
infix fun <E : Any, T : DbType<E>> Column<E, T>.default(func: () -> E): Column<E, T> {
    defaultFunction = func
    return this
}

