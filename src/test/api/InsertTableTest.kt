package api

import org.testng.Assert
import org.testng.annotations.Test
import tanvd.aorm.DbType
import tanvd.aorm.Row
import tanvd.aorm.expression.Expression
import tanvd.aorm.query.eq
import tanvd.aorm.query.where
import utils.AormTestBase
import utils.ExampleTable
import utils.getDate

class InsertTableTest: AormTestBase() {

    override fun executeBeforeMethod() {
        ExampleTable.create()
    }

    @Suppress("UNCHECKED_CAST")
    @Test
    fun insert_tableExistsRowValid_rowInserted() {
        ExampleTable.insert {
            it[ExampleTable.id] = 2L
            it[ExampleTable.value] = "value"
            it[ExampleTable.date] = getDate("2000-01-01")
            it[ExampleTable.arrayValue] = listOf("array1", "array2")
        }

        val expectedRow = Row(mapOf(ExampleTable.id to 2L, ExampleTable.value to "value",
                ExampleTable.date to getDate("2000-01-01"), ExampleTable.arrayValue to listOf("array1", "array2"))
                as Map<Expression<Any, DbType<Any>>, Any>)
        val select = ExampleTable.select() where (ExampleTable.id eq 2L)
        Assert.assertEquals(select.toResult().single(), expectedRow)
    }

    @Suppress("UNCHECKED_CAST")
    @Test
    fun insertBatch_tableExistsRowValid_rowsInserted() {
        ExampleTable.batchInsert(1..2) { table, value ->
            table[ExampleTable.id] = value
            table[ExampleTable.value] = "value"
            table[ExampleTable.date] = getDate("2000-01-01")
            table[ExampleTable.arrayValue] = listOf("array1", "array2")
        }

        val expectedRows = setOf(Row(mapOf(ExampleTable.id to 1L, ExampleTable.value to "value",
                ExampleTable.date to getDate("2000-01-01"), ExampleTable.arrayValue to listOf("array1", "array2"))
                as Map<Expression<Any, DbType<Any>>, Any>),
                Row(mapOf(ExampleTable.id to 2L, ExampleTable.value to "value",
                        ExampleTable.date to getDate("2000-01-01"), ExampleTable.arrayValue to listOf("array1", "array2"))
                as Map<Expression<Any, DbType<Any>>, Any>))
        val select = ExampleTable.select() where (ExampleTable.value eq "value")
        Assert.assertEquals(select.toResult().toSet(), expectedRows)
    }
}