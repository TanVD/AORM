package utils

import org.jetbrains.annotations.TestOnly
import tanvd.aorm.*
import tanvd.aorm.expression.alias
import tanvd.aorm.expression.default
import tanvd.aorm.query.Query

object ExampleTable : Table("ExampleTable") {
    val date = date("date")

    val id = int64("id").default { 1L }
    val value = string("value")

    val arrayValue = arrayString("string_array").default { listOf("array1", "array2") }

    override val engine = Engine.MergeTree(date, listOf(id), 8192)

    @TestOnly
    fun resetTable() {
        withDatabase(TestDatabase) {
            ignoringExceptions {
                ExampleTable.drop()
            }
        }
        ExampleTable.columns.clear()
        columns.add(date)
        columns.add(id)
        columns.add(value)
        columns.add(arrayValue)
    }
}

object ExampleView: View("ExampleView") {
    val idView = alias("id_view", ExampleTable.id)
    val valueView = alias("value_view", ExampleTable.value)

    override val query: Query = ExampleTable.select(idView, valueView)
}

object ExampleMaterializedView: MaterializedView("ExampleMaterializedView") {
    val date = alias("date_view", ExampleTable.date)
    val idView = alias("id_view", ExampleTable.id)
    val valueView = alias("value_view", ExampleTable.value)

    override val query: Query = ExampleTable.select(date, idView, valueView)

    override val engine: Engine = Engine.MergeTree(ExampleMaterializedView.date, listOf(ExampleMaterializedView.idView, ExampleMaterializedView.valueView))
}