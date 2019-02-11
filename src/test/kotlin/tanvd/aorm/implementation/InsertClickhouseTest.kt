package tanvd.aorm.implementation

import org.testng.Assert
import org.testng.annotations.Test
import tanvd.aorm.exceptions.BasicDbException
import tanvd.aorm.insert.InsertExpression
import tanvd.aorm.query.eq
import tanvd.aorm.query.where
import tanvd.aorm.utils.*
import tanvd.aorm.withDatabase

class InsertClickhouseTest : AormTestBase() {
    @Test
    fun insert_tableExistsRowValid_rowInserted() {
        withDatabase(TestDatabase) {
            ExampleTable.create()
            val row = prepareInsertRow(mapOf(ExampleTable.id to 2L, ExampleTable.value to "value",
                    ExampleTable.date to getDate("2000-01-01"), ExampleTable.arrayValue to listOf("array1", "array2"))
                    .toMutableMap())

            InsertClickhouse.insert(TestDatabase, InsertExpression(ExampleTable, ExampleTable.columns, arrayListOf(row)))

            val select = ExampleTable.select() where (ExampleTable.id eq 2L)
            AssertDb.assertEquals(select.toResult().single(), row)
        }
    }

    @Test
    fun insert_tableNotExistsRowValid_basicDbException() {
        val row = prepareInsertRow(mapOf(ExampleTable.id to 1L, ExampleTable.value to "value",
                ExampleTable.date to getDate("2000-01-01"), ExampleTable.arrayValue to listOf("array1", "array2"))
                .toMutableMap())

        try {
            InsertClickhouse.insert(TestDatabase, InsertExpression(ExampleTable, ExampleTable.columns, arrayListOf(row)))
        } catch (e: BasicDbException) {
            return
        }

        Assert.fail()
    }

    @Test
    fun insert_tableExistsRowWithDefaults_rowInserted() {
        withDatabase(TestDatabase) {

            ExampleTable.create()
            val row = prepareInsertRow(mapOf(ExampleTable.value to "value",
                    ExampleTable.date to getDate("2000-01-01")).toMutableMap())

            InsertClickhouse.insert(TestDatabase, InsertExpression(ExampleTable, ExampleTable.columns, arrayListOf(row)))

            val select = ExampleTable.select() where (ExampleTable.id eq 1L)
            val rowGot = prepareSelectRow(mapOf(ExampleTable.id to 1L, ExampleTable.value to "value",
                    ExampleTable.date to getDate("2000-01-01"),
                    ExampleTable.arrayValue to listOf("array1", "array2")).toMutableMap())
            Assert.assertEquals(select.toResult().single(), rowGot)
        }
    }

    @Test
    fun insert_tableExistsRowsValid_rowsInserted() {
        withDatabase(TestDatabase) {

            ExampleTable.create()
            val rows = arrayListOf(
                    prepareInsertRow(mapOf(ExampleTable.id to 2L, ExampleTable.value to "value",
                            ExampleTable.date to getDate("2000-01-01"),
                            ExampleTable.arrayValue to listOf("array1", "array2")).toMutableMap()),
                    prepareInsertRow(mapOf(ExampleTable.id to 3L, ExampleTable.value to "value",
                            ExampleTable.date to getDate("2000-02-02"),
                            ExampleTable.arrayValue to listOf("array3", "array4")).toMutableMap())
            )

            InsertClickhouse.insert(TestDatabase, InsertExpression(ExampleTable, ExampleTable.columns, rows))

            val select = ExampleTable.select() where (ExampleTable.value eq "value")
            AssertDb.assertEquals(select.toResult().toSet(), rows.toSet())
        }
    }

    @Test
    fun insert_tableExistsRowsWithDefaults_rowsInserted() {
        withDatabase(TestDatabase) {
            ExampleTable.create()
            val rows = arrayListOf(
                    prepareInsertRow(mapOf(ExampleTable.value to "value1",
                            ExampleTable.date to getDate("2000-01-01")).toMutableMap()),
                    prepareInsertRow(mapOf(ExampleTable.value to "value2",
                            ExampleTable.date to getDate("2000-02-02")).toMutableMap())
            )

            InsertClickhouse.insert(TestDatabase, InsertExpression(ExampleTable, ExampleTable.columns, rows))

            val gotRows = arrayListOf(
                    prepareSelectRow(mapOf(ExampleTable.id to 1L, ExampleTable.value to "value1",
                            ExampleTable.date to getDate("2000-01-01"),
                            ExampleTable.arrayValue to listOf("array1", "array2")).toMutableMap()),
                    prepareSelectRow(mapOf(ExampleTable.id to 1L, ExampleTable.value to "value2",
                            ExampleTable.date to getDate("2000-02-02"),
                            ExampleTable.arrayValue to listOf("array1", "array2")).toMutableMap())
            )
            val select = ExampleTable.select() where (ExampleTable.id eq 1L)
            Assert.assertEquals(select.toResult().toSet(), gotRows.toSet())
        }
    }

    @Test
    fun constructInsert_oneRow_equalsToPredefined() {
        withDatabase(TestDatabase) {

            ExampleTable.create()
            val row = prepareInsertRow(mapOf(ExampleTable.date to getDate("2000-02-02"), ExampleTable.id to 3L,
                    ExampleTable.value to "value").toMutableMap())

            val sql = InsertClickhouse.constructInsert(InsertExpression(ExampleTable, ExampleTable.columns, arrayListOf(row)))

            Assert.assertEquals(sql, "INSERT INTO ExampleTable (date, id, value, string_array) VALUES ('2000-02-02', 3," +
                    " 'value', ['array1', 'array2']);")
        }
    }

    @Test
    fun constructInsert_twoRows_equalsToPredefined() {
        withDatabase(TestDatabase) {

            ExampleTable.create()
            val rows = arrayListOf(
                    prepareInsertRow(mapOf(ExampleTable.id to 2L, ExampleTable.value to "value",
                            ExampleTable.date to getDate("2000-01-01")).toMutableMap()),
                    prepareInsertRow(mapOf(ExampleTable.id to 3L, ExampleTable.value to "value",
                            ExampleTable.date to getDate("2000-02-02")).toMutableMap())
            )

            val sql = InsertClickhouse.constructInsert(InsertExpression(ExampleTable, ExampleTable.columns, rows))

            Assert.assertEquals(sql, "INSERT INTO ExampleTable (date, id, value, string_array) VALUES ('2000-01-01', 2, 'value', ['array1', 'array2']), ('2000-02-02', 3, 'value', ['array1', 'array2']);")
        }
    }
}
