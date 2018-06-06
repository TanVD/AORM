package tanvd.aorm

import tanvd.aorm.expression.Column
import tanvd.aorm.implementation.InsertClickhouse

data class InsertExpression(val table: Table, val columns: List<Column<Any, DbType<Any>>>,
                            val values: MutableList<Row>) {
    constructor(table: Table, row: Row) : this(table, (row.columns + table.columnsWithDefaults).distinct() as List<Column<Any, DbType<Any>>>, arrayListOf(row))

    fun toSql() = InsertClickhouse.constructInsert(this)
}
