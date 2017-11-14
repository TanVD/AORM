package utils

import tanvd.aorm.*

object ExampleTable: Table("ExampleTable", TestDatabase) {
    val date = date("date")

    val id = long("id").default { 1L }
    val value = string("value")

    val arrayValue = arrayString("string_array")

    override val engine: Engine = MergeTree(date, listOf(id), 8192)

    //For tests
    fun resetTable() {
        ExampleTable.drop()
        ExampleTable.columns.clear()
        columns.add(date as Column<Any, DbType<Any>>)
        columns.add(id as Column<Any, DbType<Any>>)
        columns.add(value as Column<Any, DbType<Any>>)
    }
}