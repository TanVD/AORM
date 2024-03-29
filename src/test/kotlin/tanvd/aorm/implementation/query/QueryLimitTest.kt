package tanvd.aorm.implementation.query

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import tanvd.aorm.implementation.InsertClickhouse
import tanvd.aorm.insert.InsertExpression
import tanvd.aorm.query.*
import tanvd.aorm.utils.*
import tanvd.aorm.withDatabase

class QueryLimitTest : AormTestBase() {
    override fun executeBeforeMethod() {
        withDatabase(database) {
            ExampleTable.create()
        }
    }

    @Test
    fun limit_limitByOneOrdered_gotOneRow() {
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

            val select = (ExampleTable.select() where (ExampleTable.value eq "value")).orderBy(ExampleTable.id to Order.ASC).limit(1L, 0L)

            AssertDb.assertEquals(select.toResult().single(), rows.first())
        }
    }

    @Test
    fun limit_limitByZero_gotNoRows() {
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

            val select = (ExampleTable.select() where (ExampleTable.value eq "value")).orderBy(ExampleTable.id to Order.DESC).limit(0L, 0L)

            Assertions.assertTrue(select.toResult().isEmpty())
        }
    }

    @Test
    fun limit_limitByTwo_gotAllRows() {
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

            val select = (ExampleTable.select() where (ExampleTable.value eq "value")).orderBy(ExampleTable.id to Order.DESC).limit(2L, 0L)

            AssertDb.assertEquals(select.toResult(), rows)
        }
    }

    @Test
    fun limit_orderedByTwoLimitByTwo_gotAllRows() {
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
                    ExampleTable.value to Order.DESC).limit(2L, 0L)

            AssertDb.assertEquals(select.toResult(), rows)
        }
    }
}
