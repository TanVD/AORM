package tanvd.aorm.implementation.query

import org.testng.Assert
import org.testng.annotations.Test
import tanvd.aorm.expression.count
import tanvd.aorm.implementation.InsertClickhouse
import tanvd.aorm.insert.InsertExpression
import tanvd.aorm.query.*
import tanvd.aorm.utils.*
import tanvd.aorm.withDatabase


@Suppress("UNCHECKED_CAST")
class QueryGroupByTest : AormTestBase() {

    override fun executeBeforeMethod() {
        withDatabase(database) {
            ExampleTable.create()
        }
    }

    @Test
    fun groupBy_oneGroupCountTwo_gotOneRowWithTwo() {
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

            val select = ExampleTable.select(ExampleTable.value, count(ExampleTable.date)).groupBy(ExampleTable.value).toResult()

            Assert.assertEquals(select.single(), prepareSelectRow(mapOf(count(ExampleTable.date) to 2L, ExampleTable.value to "value")))
        }
    }

    @Test
    fun groupBy_twoGroupsCountOne_gotTwoRowsWithOne() {
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

            val select = ExampleTable.select(ExampleTable.id, count(ExampleTable.id)).orderBy(ExampleTable.id to Order.ASC).groupBy(ExampleTable.id).toResult()

            Assert.assertEquals(select, listOf(
                    prepareSelectRow(mapOf(ExampleTable.id to 2L, count(ExampleTable.id) to 1L)),
                    prepareSelectRow(mapOf(ExampleTable.id to 3L, count(ExampleTable.id) to 1L))))
        }
    }
}
