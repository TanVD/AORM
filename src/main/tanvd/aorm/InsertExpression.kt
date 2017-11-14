package tanvd.aorm

import tanvd.aorm.implementation.InsertClickhouse

data class InsertExpression(val table: Table, val columns: List<Column<Any, DbType<Any>>>, val values: MutableList<Row>) {
    constructor(table: Table, row: Row): this(table, (row.columns + table.columnsWithDefaults).distinct(), arrayListOf(row))

    fun toSql(): String {
        return InsertClickhouse.constructInsert(this)
    }
}
