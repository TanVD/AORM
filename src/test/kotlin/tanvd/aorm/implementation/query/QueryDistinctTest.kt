package tanvd.aorm.implementation.query

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import tanvd.aorm.implementation.InsertClickhouse
import tanvd.aorm.insert.InsertExpression
import tanvd.aorm.query.distinct
import tanvd.aorm.utils.AormTestBase
import tanvd.aorm.utils.ExampleTable
import tanvd.aorm.utils.getDate
import tanvd.aorm.withDatabase

class QueryDistinctTest : AormTestBase() {

    override fun executeBeforeMethod() {
        withDatabase(database) {
            ExampleTable.create()
        }
    }

    @Test
    fun `distinct with single column`() {
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

            val expectedResultRow = prepareSelectRow(mapOf(ExampleTable.value to "value"))
            val selectWithoutDistinct = ExampleTable.select(ExampleTable.value).toResult()
            Assertions.assertEquals(2, selectWithoutDistinct.size)
            Assertions.assertTrue(selectWithoutDistinct.all{ it == expectedResultRow })

            val selectWithDistinct = ExampleTable.select(ExampleTable.value).distinct().toResult()
            Assertions.assertEquals(1, selectWithDistinct.size)
            Assertions.assertEquals(expectedResultRow, selectWithDistinct.single())
        }
    }

}