package tanvd.aorm.insert

import tanvd.aorm.*
import tanvd.aorm.expression.Column
import tanvd.aorm.implementation.InsertClickhouse

data class InsertExpression(val table: Table, val columns: Set<Column<*, DbType<*>>>,
                            val values: MutableList<InsertRow>) {
    constructor(table: Table, row: InsertRow) : this(table, row.columns + table.columnsWithDefaults, arrayListOf(row))

    fun toSql() = InsertClickhouse.constructInsert(this)
}
