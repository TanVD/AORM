package tanvd.aorm

import tanvd.aorm.implementation.InsertClickhouse

data class InsertExpression(val table: Table, val columns: List<Column<Any, DbType<Any>>>, val values: MutableList<Row>) {
    constructor(table: Table, row: Row): this(table, (row.columns + table.columnsWithDefaults).distinct(), arrayListOf(row))

    fun toSql(): String {
        return InsertClickhouse.constructInsert(this)
    }
}

//Helper functions

/**
 * Will insert in table all specified values and columns with default functions.
 * Other columns will be default by CH
 */
infix fun Table.insert(values: Row) {
    return this.insert(InsertExpression(this, (values.columns + columnsWithDefaults).distinct(), mutableListOf(values)))
}

infix fun <E: Any, T: DbType<E>>Table.insertWithColumns(columns: List<Column<E, T>>): InsertExpression {
    return InsertExpression(this, columns as List<Column<Any, DbType<Any>>>, ArrayList())
}

infix fun InsertExpression.values(values: List<Row>) {
    this.values.addAll(values)
    this.table.insert(this)
}
