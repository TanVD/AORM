package tanvd.aorm

import tanvd.aorm.expression.Column
import tanvd.aorm.implementation.InsertClickhouse

data class InsertExpression(val table: Table, val columns: List<Column<Any, DbType<Any>>>,
                            val values: MutableList<InsertRow>) {
    constructor(table: Table, row: InsertRow) : this(table, (row.columns + table.columnsWithDefaults).distinct(), arrayListOf(row))

    fun toSql() = InsertClickhouse.constructInsert(this)
}
