package implementation.query

import org.testng.Assert
import org.testng.annotations.Test
import tanvd.aorm.DbType
import tanvd.aorm.InsertExpression
import tanvd.aorm.Row
import tanvd.aorm.expression.Column
import tanvd.aorm.implementation.InsertClickhouse
import tanvd.aorm.query.*
import tanvd.aorm.withDatabase
import utils.AormTestBase
import utils.ExampleTable
import utils.TestDatabase
import utils.getDate

@Suppress("UNCHECKED_CAST")
class QueryLoadTest : AormTestBase() {
    override fun executeBeforeMethod() {
        withDatabase(TestDatabase) {
            ExampleTable.create()
        }
    }

    @Test
    fun where_validQuery_gotRow() {
        withDatabase(TestDatabase) {

            val row = Row(mapOf(
                    ExampleTable.id to 1L,
                    ExampleTable.value to "value",
                    ExampleTable.date to getDate("2000-01-01")).toMutableMap())
            InsertClickhouse.insert(TestDatabase, InsertExpression(ExampleTable, ExampleTable.columns as List<Column<Any, DbType<Any>>>,
                    arrayListOf(row)))

            val select = ExampleTable.select() where (ExampleTable.id eq 1L)

            val rowGot = Row(mapOf(ExampleTable.id to 1L, ExampleTable.value to "value",
                    ExampleTable.date to getDate("2000-01-01"),
                    ExampleTable.arrayValue to listOf("array1", "array2")).toMutableMap())
            Assert.assertEquals(select.toResult().single(), rowGot)
        }
    }

    @Test
    fun prewhere_validQuery_gotRow() {
        withDatabase(TestDatabase) {

            val row = Row(mapOf(
                    ExampleTable.id to 1L,
                    ExampleTable.value to "value",
                    ExampleTable.date to getDate("2000-01-01")).toMutableMap())
            InsertClickhouse.insert(TestDatabase, InsertExpression(ExampleTable, ExampleTable.columns as List<Column<Any, DbType<Any>>>,
                    arrayListOf(row)))

            val select = ExampleTable.select() prewhere (ExampleTable.id eq 1L)

            val rowGot = Row(mapOf(ExampleTable.id to 1L, ExampleTable.value to "value",
                    ExampleTable.date to getDate("2000-01-01"),
                    ExampleTable.arrayValue to listOf("array1", "array2")).toMutableMap())
            Assert.assertEquals(select.toResult().single(), rowGot)
        }
    }

    @Test
    fun complexQuery_validQuery_gotRow() {
        withDatabase(TestDatabase) {

            val row = Row(mapOf(
                    ExampleTable.id to 1L,
                    ExampleTable.value to "value",
                    ExampleTable.date to getDate("2000-01-01")).toMutableMap())
            InsertClickhouse.insert(TestDatabase, InsertExpression(ExampleTable, ExampleTable.columns as List<Column<Any, DbType<Any>>>,
                    arrayListOf(row)))

            val select = (ExampleTable.select() prewhere (ExampleTable.id eq 1L) where (ExampleTable.value eq "value"))
                    .orderBy(ExampleTable.arrayValue to Order.ASC, ExampleTable.date to Order.DESC).limit(1, 0)

            val rowGot = Row(mapOf(ExampleTable.id to 1L, ExampleTable.value to "value",
                    ExampleTable.date to getDate("2000-01-01"),
                    ExampleTable.arrayValue to listOf("array1", "array2")).toMutableMap())
            Assert.assertEquals(select.toResult().single(), rowGot)
        }

    }
}