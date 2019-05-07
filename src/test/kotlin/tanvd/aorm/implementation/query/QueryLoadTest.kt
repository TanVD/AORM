package tanvd.aorm.implementation.query

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import tanvd.aorm.implementation.InsertClickhouse
import tanvd.aorm.insert.InsertExpression
import tanvd.aorm.query.*
import tanvd.aorm.utils.*
import tanvd.aorm.withDatabase

class QueryLoadTest : AormTestBase() {
    override fun executeBeforeMethod() {
        withDatabase(database) {
            ExampleTable.create()
        }
    }

    @Test
    fun where_validQuery_gotRow() {
        withDatabase(database) {

            val row = prepareInsertRow(mapOf(
                    ExampleTable.id to 1L,
                    ExampleTable.value to "value",
                    ExampleTable.date to getDate("2000-01-01")))
            InsertClickhouse.insert(database, InsertExpression(ExampleTable, ExampleTable.columns,
                    arrayListOf(row)))

            val select = ExampleTable.select() where (ExampleTable.id eq 1L)

            val rowGot = prepareSelectRow(mapOf(ExampleTable.id to 1L, ExampleTable.value to "value",
                    ExampleTable.date to getDate("2000-01-01"),
                    ExampleTable.arrayValue to listOf("array1", "array2")))
            Assertions.assertEquals(select.toResult().single(), rowGot)
        }
    }

    @Test
    fun prewhere_validQuery_gotRow() {
        withDatabase(database) {

            val row = prepareInsertRow(mapOf(
                    ExampleTable.id to 1L,
                    ExampleTable.value to "value",
                    ExampleTable.date to getDate("2000-01-01")))
            InsertClickhouse.insert(database, InsertExpression(ExampleTable, ExampleTable.columns,
                    arrayListOf(row)))

            val select = ExampleTable.select() prewhere (ExampleTable.id eq 1L)

            val rowGot = prepareSelectRow(mapOf(ExampleTable.id to 1L, ExampleTable.value to "value",
                    ExampleTable.date to getDate("2000-01-01"),
                    ExampleTable.arrayValue to listOf("array1", "array2")))
            Assertions.assertEquals(select.toResult().single(), rowGot)
        }
    }

    @Test
    fun complexQuery_validQuery_gotRow() {
        withDatabase(database) {

            val row = prepareInsertRow(mapOf(
                    ExampleTable.id to 1L,
                    ExampleTable.value to "value",
                    ExampleTable.date to getDate("2000-01-01")))
            InsertClickhouse.insert(database, InsertExpression(ExampleTable, ExampleTable.columns,
                    arrayListOf(row)))

            val select = (ExampleTable.select() prewhere (ExampleTable.id eq 1L) where (ExampleTable.value eq "value"))
                    .orderBy(ExampleTable.arrayValue to Order.ASC, ExampleTable.date to Order.DESC).limit(1, 0)

            val rowGot = prepareSelectRow(mapOf(ExampleTable.id to 1L, ExampleTable.value to "value",
                    ExampleTable.date to getDate("2000-01-01"),
                    ExampleTable.arrayValue to listOf("array1", "array2")))
            Assertions.assertEquals(select.toResult().single(), rowGot)
        }

    }
}
