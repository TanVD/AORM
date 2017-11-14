package implementation.query

import org.testng.Assert
import org.testng.annotations.Test
import tanvd.aorm.Column
import tanvd.aorm.DbType
import tanvd.aorm.InsertExpression
import tanvd.aorm.Row
import tanvd.aorm.implementation.InsertClickhouse
import tanvd.aorm.query.*
import utils.AormTestBase
import utils.ExampleTable
import utils.getDate

@Suppress("UNCHECKED_CAST")
class QueryLoadTest : AormTestBase() {
    override fun executeBeforeMethod() {
        ExampleTable.create()
    }

    @Test
    fun where_validQuery_gotRow() {
            val row = Row(mapOf(
                    ExampleTable.id to 1L,
                    ExampleTable.value to "value",
                    ExampleTable.date to getDate("2000-01-01")) as Map<Column<Any, DbType<Any>>, Any>)
            InsertClickhouse.insert(InsertExpression(ExampleTable, row))

            val select = ExampleTable.select() where (ExampleTable.id eq 1L)

            val rowGot = Row(mapOf(ExampleTable.id to 1L, ExampleTable.value to "value",
                    ExampleTable.date to getDate("2000-01-01"),
                    ExampleTable.arrayValue to listOf("array1", "array2")) as Map<Column<Any, DbType<Any>>, Any>)
            Assert.assertEquals(select.toResult().single(), rowGot)
    }

    @Test
    fun prewhere_validQuery_gotRow() {
        val row = Row(mapOf(
                ExampleTable.id to 1L,
                ExampleTable.value to "value",
                ExampleTable.date to getDate("2000-01-01")) as Map<Column<Any, DbType<Any>>, Any>)
        InsertClickhouse.insert(InsertExpression(ExampleTable, row))

        val select = ExampleTable.select() prewhere (ExampleTable.id eq 1L)

        val rowGot = Row(mapOf(ExampleTable.id to 1L, ExampleTable.value to "value",
                ExampleTable.date to getDate("2000-01-01"),
                ExampleTable.arrayValue to listOf("array1", "array2")) as Map<Column<Any, DbType<Any>>, Any>)
        Assert.assertEquals(select.toResult().single(), rowGot)
    }

    @Test
    fun complexQuery_validQuery_gotRow() {
        val row = Row(mapOf(
                ExampleTable.id to 1L,
                ExampleTable.value to "value",
                ExampleTable.date to getDate("2000-01-01")) as Map<Column<Any, DbType<Any>>, Any>)
        InsertClickhouse.insert(InsertExpression(ExampleTable, row))

        val select = (ExampleTable.select() prewhere (ExampleTable.id eq 1L) where (ExampleTable.value eq "value"))
                .orderBy(ExampleTable.arrayValue to Order.ASC, ExampleTable.date to Order.DESC).limit(1, 0)

        val rowGot = Row(mapOf(ExampleTable.id to 1L, ExampleTable.value to "value",
                ExampleTable.date to getDate("2000-01-01"),
                ExampleTable.arrayValue to listOf("array1", "array2")) as Map<Column<Any, DbType<Any>>, Any>)
        Assert.assertEquals(select.toResult().single(), rowGot)

    }
}