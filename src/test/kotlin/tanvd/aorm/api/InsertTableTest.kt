package tanvd.aorm.api

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import tanvd.aorm.query.eq
import tanvd.aorm.query.where
import tanvd.aorm.utils.*
import tanvd.aorm.withDatabase

class InsertTableTest : AormTestBase() {
    override fun executeBeforeMethod() {
        withDatabase(database) {
            tryRun { ExampleTable.drop() }
            ExampleTable.create()
        }
    }

    @Test
    fun insert_tableExistsRowValid_rowInserted() {
        withDatabase(database) {
            ExampleTable.insert {
                it[ExampleTable.id] = 2L
                it[ExampleTable.value] = "value"
                it[ExampleTable.date] = getDate("2000-01-01")
                it[ExampleTable.arrayValue] = listOf("array1", "array2")
            }


            val expectedRow = prepareInsertRow(mapOf(ExampleTable.id to 2L, ExampleTable.value to "value",
                    ExampleTable.date to getDate("2000-01-01"), ExampleTable.arrayValue to listOf("array1", "array2")))
            val select = ExampleTable.select() where (ExampleTable.id eq 2L)
            AssertDb.assertEquals(select.toResult().single(), expectedRow)
        }
    }

    @Test
    fun insertBatch_tableExistsRowValid_rowsInserted() {
        withDatabase(database) {

            ExampleTable.batchInsert(1..2) { table, value ->
                table[ExampleTable.id] = value.toLong()
                table[ExampleTable.value] = "value"
                table[ExampleTable.date] = getDate("2000-01-01")
                table[ExampleTable.arrayValue] = listOf("array1", "array2")
            }

            val expectedRows = setOf(prepareInsertRow(mapOf(ExampleTable.id to 1L, ExampleTable.value to "value",
                    ExampleTable.date to getDate("2000-01-01"), ExampleTable.arrayValue to listOf("array1", "array2"))),
                    prepareInsertRow(mapOf(ExampleTable.id to 2L, ExampleTable.value to "value",
                            ExampleTable.date to getDate("2000-01-01"), ExampleTable.arrayValue to listOf("array1", "array2"))))
            val select = ExampleTable.select() where (ExampleTable.value eq "value")
            AssertDb.assertEquals(select.toResult().toSet(), expectedRows)
        }
    }

    @Test
    fun insertLazy_tableExistsRowsValid_rowsInsertedAfterDelay() {
        withDatabase(database, insertWorker) {
            ExampleTable.insertLazy { table ->
                table[ExampleTable.id] = 1L
                table[ExampleTable.value] = "value"
                table[ExampleTable.date] = getDate("2000-01-01")
                table[ExampleTable.arrayValue] = listOf("array1", "array2")
            }

            ExampleTable.insertLazy { table ->
                table[ExampleTable.id] = 2L
                table[ExampleTable.value] = "value"
                table[ExampleTable.date] = getDate("2000-01-01")
                table[ExampleTable.arrayValue] = listOf("array1", "array2")
            }

            Thread.sleep(testInsertWorkerDelayMs + 1000)

            val expectedRows = setOf(prepareInsertRow(mapOf(ExampleTable.id to 1L, ExampleTable.value to "value",
                    ExampleTable.date to getDate("2000-01-01"), ExampleTable.arrayValue to listOf("array1", "array2"))),
                    prepareInsertRow(mapOf(ExampleTable.id to 2L, ExampleTable.value to "value",
                            ExampleTable.date to getDate("2000-01-01"), ExampleTable.arrayValue to listOf("array1", "array2"))))
            val select = ExampleTable.select() where (ExampleTable.value eq "value")
            AssertDb.assertEquals(select.toResult().toSet(), expectedRows)
        }
    }

    @Test
    fun insertLazy_tableExistsRowsValid_rowsNotInsertedBeforeDelay() {
        withDatabase(database, insertWorker) {
            ExampleTable.insertLazy { table ->
                table[ExampleTable.id] = 1L
                table[ExampleTable.value] = "value"
                table[ExampleTable.date] = getDate("2000-01-01")
                table[ExampleTable.arrayValue] = listOf("array1", "array2")
            }

            ExampleTable.insertLazy { table ->
                table[ExampleTable.id] = 2L
                table[ExampleTable.value] = "value"
                table[ExampleTable.date] = getDate("2000-01-01")
                table[ExampleTable.arrayValue] = listOf("array1", "array2")
            }

            val select = ExampleTable.select() where (ExampleTable.value eq "value")
            Assertions.assertTrue(select.toResult().isEmpty())
        }
    }
}
