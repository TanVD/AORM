package implementation.query

import org.testng.Assert
import org.testng.annotations.Test
import tanvd.aorm.insert.InsertExpression
import tanvd.aorm.implementation.InsertClickhouse
import tanvd.aorm.query.*
import tanvd.aorm.withDatabase
import utils.AormTestBase
import utils.ExampleTable
import utils.TestDatabase
import utils.getDate

class QueryLoadTest : AormTestBase() {
    override fun executeBeforeMethod() {
        withDatabase(TestDatabase) {
            ExampleTable.create()
        }
    }

    @Test
    fun where_validQuery_gotRow() {
        withDatabase(TestDatabase) {

            val row = prepareInsertRow(mapOf(
                    ExampleTable.id to 1L,
                    ExampleTable.value to "value",
                    ExampleTable.date to getDate("2000-01-01")))
            InsertClickhouse.insert(TestDatabase, InsertExpression(ExampleTable, ExampleTable.columns,
                    arrayListOf(row)))

            val select = ExampleTable.select() where (ExampleTable.id eq 1L)

            val rowGot = prepareSelectRow(mapOf(ExampleTable.id to 1L, ExampleTable.value to "value",
                    ExampleTable.date to getDate("2000-01-01"),
                    ExampleTable.arrayValue to listOf("array1", "array2")))
            Assert.assertEquals(select.toResult().single(), rowGot)
        }
    }

    @Test
    fun prewhere_validQuery_gotRow() {
        withDatabase(TestDatabase) {

            val row = prepareInsertRow(mapOf(
                    ExampleTable.id to 1L,
                    ExampleTable.value to "value",
                    ExampleTable.date to getDate("2000-01-01")))
            InsertClickhouse.insert(TestDatabase, InsertExpression(ExampleTable, ExampleTable.columns,
                    arrayListOf(row)))

            val select = ExampleTable.select() prewhere (ExampleTable.id eq 1L)

            val rowGot = prepareSelectRow(mapOf(ExampleTable.id to 1L, ExampleTable.value to "value",
                    ExampleTable.date to getDate("2000-01-01"),
                    ExampleTable.arrayValue to listOf("array1", "array2")))
            Assert.assertEquals(select.toResult().single(), rowGot)
        }
    }

    @Test
    fun complexQuery_validQuery_gotRow() {
        withDatabase(TestDatabase) {

            val row = prepareInsertRow(mapOf(
                    ExampleTable.id to 1L,
                    ExampleTable.value to "value",
                    ExampleTable.date to getDate("2000-01-01")))
            InsertClickhouse.insert(TestDatabase, InsertExpression(ExampleTable, ExampleTable.columns,
                    arrayListOf(row)))

            val select = (ExampleTable.select() prewhere (ExampleTable.id eq 1L) where (ExampleTable.value eq "value"))
                    .orderBy(ExampleTable.arrayValue to Order.ASC, ExampleTable.date to Order.DESC).limit(1, 0)

            val rowGot = prepareSelectRow(mapOf(ExampleTable.id to 1L, ExampleTable.value to "value",
                    ExampleTable.date to getDate("2000-01-01"),
                    ExampleTable.arrayValue to listOf("array1", "array2")))
            Assert.assertEquals(select.toResult().single(), rowGot)
        }

    }
}