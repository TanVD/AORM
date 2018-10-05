package implementation.query

import org.testng.Assert
import org.testng.annotations.Test
import tanvd.aorm.insert.InsertExpression
import tanvd.aorm.expression.count
import tanvd.aorm.implementation.InsertClickhouse
import tanvd.aorm.query.Order
import tanvd.aorm.query.groupBy
import tanvd.aorm.query.orderBy
import tanvd.aorm.withDatabase
import utils.AormTestBase
import utils.ExampleTable
import utils.TestDatabase
import utils.getDate


@Suppress("UNCHECKED_CAST")
class QueryGroupByTest : AormTestBase() {

    override fun executeBeforeMethod() {
        withDatabase(TestDatabase) {
            ExampleTable.create()
        }
    }

    @Test
    fun groupBy_oneGroupCountTwo_gotOneRowWithTwo() {
        withDatabase(TestDatabase) {
            val rows = arrayListOf(
                    prepareInsertRow(mapOf(ExampleTable.id to 2L, ExampleTable.value to "value",
                            ExampleTable.date to getDate("2000-01-01"),
                            ExampleTable.arrayValue to listOf("array1", "array2"))),
                    prepareInsertRow(mapOf(ExampleTable.id to 3L, ExampleTable.value to "value",
                            ExampleTable.date to getDate("2000-02-02"),
                            ExampleTable.arrayValue to listOf("array3", "array4")))
            )
            InsertClickhouse.insert(TestDatabase, InsertExpression(ExampleTable, ExampleTable.columns, rows))

            val select = ExampleTable.select(ExampleTable.value, count(ExampleTable.date)).groupBy(ExampleTable.value).toResult()

            Assert.assertEquals(select.single(), prepareSelectRow(mapOf(count(ExampleTable.date) to 2L, ExampleTable.value to "value")))
        }
    }

    @Test
    fun groupBy_twoGroupsCountOne_gotTwoRowsWithOne() {
        withDatabase(TestDatabase) {
            val rows = arrayListOf(
                    prepareInsertRow(mapOf(ExampleTable.id to 2L, ExampleTable.value to "value",
                            ExampleTable.date to getDate("2000-01-01"),
                            ExampleTable.arrayValue to listOf("array1", "array2"))),
                    prepareInsertRow(mapOf(ExampleTable.id to 3L, ExampleTable.value to "value",
                            ExampleTable.date to getDate("2000-02-02"),
                            ExampleTable.arrayValue to listOf("array3", "array4")))
            )
            InsertClickhouse.insert(TestDatabase, InsertExpression(ExampleTable, ExampleTable.columns, rows))

            val select = ExampleTable.select(ExampleTable.id, count(ExampleTable.id)).orderBy(ExampleTable.id to Order.ASC).groupBy(ExampleTable.id).toResult()

            Assert.assertEquals(select, listOf(
                    prepareSelectRow(mapOf(ExampleTable.id to 2L, count(ExampleTable.id) to 1L)),
                    prepareSelectRow(mapOf(ExampleTable.id to 3L, count(ExampleTable.id) to 1L))))
        }
    }
}