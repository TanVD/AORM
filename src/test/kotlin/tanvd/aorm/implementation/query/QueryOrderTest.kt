package tanvd.aorm.implementation.query

import org.junit.jupiter.api.Test
import tanvd.aorm.implementation.InsertClickhouse
import tanvd.aorm.insert.InsertExpression
import tanvd.aorm.query.*
import tanvd.aorm.utils.*
import tanvd.aorm.withDatabase

@Suppress("UNCHECKED_CAST")
class QueryOrderTest : AormTestBase() {
    override fun executeBeforeMethod() {
        withDatabase(database) {
            ExampleTable.create()
        }
    }

    @Test
    fun orderBy_orderingByIdAsc_gotOrderedAsc() {
        withDatabase(database) {
            val rows = arrayListOf(
                    prepareInsertRow(mapOf(ExampleTable.id to 2L, ExampleTable.value to "value",
                            ExampleTable.date to getDate("2000-01-01"),
                            ExampleTable.arrayValue to listOf("array1", "array2"))),
                    prepareInsertRow(mapOf(ExampleTable.id to 3L, ExampleTable.value to "value",
                            ExampleTable.date to getDate("2000-02-02"),
                            ExampleTable.arrayValue to listOf("array3", "array4")))
            )

            InsertClickhouse.insert(database, InsertExpression(ExampleTable, ExampleTable.columns, rows))

            val select = (ExampleTable.select() where (ExampleTable.value eq "value")).orderBy(ExampleTable.id to Order.ASC)
            AssertDb.assertEquals(select.toResult(), rows)
        }
    }

    @Test
    fun orderBy_orderingByIdDesc_gotOrderedDesc() {
        withDatabase(database) {
            val rows = arrayListOf(
                    prepareInsertRow(mapOf(ExampleTable.id to 3L, ExampleTable.value to "value",
                            ExampleTable.date to getDate("2000-01-01"),
                            ExampleTable.arrayValue to listOf("array1", "array2"))),
                    prepareInsertRow(mapOf(ExampleTable.id to 2L, ExampleTable.value to "value",
                            ExampleTable.date to getDate("2000-02-02"),
                            ExampleTable.arrayValue to listOf("array3", "array4")))
            )

            InsertClickhouse.insert(database, InsertExpression(ExampleTable, ExampleTable.columns, rows))

            val select = (ExampleTable.select() where (ExampleTable.value eq "value")).orderBy(ExampleTable.id to Order.DESC)
            AssertDb.assertEquals(select.toResult(), rows)
        }
    }

    @Test
    fun orderBy_orderingByTwoColumns_gotOrdered() {
        withDatabase(database) {
            val rows = arrayListOf(
                    prepareInsertRow(mapOf(ExampleTable.id to 3L, ExampleTable.value to "value",
                            ExampleTable.date to getDate("2000-01-01"),
                            ExampleTable.arrayValue to listOf("array1", "array2"))),
                    prepareInsertRow(mapOf(ExampleTable.id to 2L, ExampleTable.value to "value",
                            ExampleTable.date to getDate("2000-02-02"),
                            ExampleTable.arrayValue to listOf("array3", "array4")))
            )

            InsertClickhouse.insert(database, InsertExpression(ExampleTable, ExampleTable.columns, rows))

            val select = (ExampleTable.select() where (ExampleTable.value eq "value")).orderBy(ExampleTable.id to Order.DESC,
                    ExampleTable.date to Order.DESC)
            AssertDb.assertEquals(select.toResult(), rows)
        }
    }
}
