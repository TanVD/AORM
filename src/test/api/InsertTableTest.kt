package api

import org.testng.Assert
import org.testng.annotations.Test
import tanvd.aorm.DbType
import tanvd.aorm.Row
import tanvd.aorm.expression.Expression
import tanvd.aorm.query.eq
import tanvd.aorm.query.where
import tanvd.aorm.withDatabase
import utils.*

class InsertTableTest : AormTestBase() {

    override fun executeBeforeMethod() {
        withDatabase(TestDatabase) {
            ignoringExceptions { ExampleTable.drop() }
            ExampleTable.create()
        }
    }

    @Suppress("UNCHECKED_CAST")
    @Test
    fun insert_tableExistsRowValid_rowInserted() {
        withDatabase(TestDatabase) {
            ExampleTable.insert {
                it[ExampleTable.id] = 2L
                it[ExampleTable.value] = "value"
                it[ExampleTable.date] = getDate("2000-01-01")
                it[ExampleTable.arrayValue] = listOf("array1", "array2")
            }


            val expectedRow = Row(mapOf(ExampleTable.id to 2L, ExampleTable.value to "value",
                    ExampleTable.date to getDate("2000-01-01"), ExampleTable.arrayValue to listOf("array1", "array2")).toMutableMap())
            val select = ExampleTable.select() where (ExampleTable.id eq 2L)
            Assert.assertEquals(select.toResult().single(), expectedRow)
        }
    }

    @Suppress("UNCHECKED_CAST")
    @Test
    fun insertBatch_tableExistsRowValid_rowsInserted() {
        withDatabase(TestDatabase) {

            ExampleTable.batchInsert(1..2) { table, value ->
                table[ExampleTable.id] = value.toLong()
                table[ExampleTable.value] = "value"
                table[ExampleTable.date] = getDate("2000-01-01")
                table[ExampleTable.arrayValue] = listOf("array1", "array2")
            }

            val expectedRows = setOf(Row(mapOf(ExampleTable.id to 1L, ExampleTable.value to "value",
                    ExampleTable.date to getDate("2000-01-01"), ExampleTable.arrayValue to listOf("array1", "array2")).toMutableMap()),
                    Row(mapOf(ExampleTable.id to 2L, ExampleTable.value to "value",
                            ExampleTable.date to getDate("2000-01-01"), ExampleTable.arrayValue to listOf("array1", "array2")).toMutableMap()))
            val select = ExampleTable.select() where (ExampleTable.value eq "value")
            Assert.assertEquals(select.toResult().toSet(), expectedRows)
        }
    }
}