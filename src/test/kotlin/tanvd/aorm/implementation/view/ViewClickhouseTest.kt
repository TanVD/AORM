package tanvd.aorm.implementation.view

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import tanvd.aorm.implementation.InsertClickhouse
import tanvd.aorm.insert.InsertExpression
import tanvd.aorm.utils.*
import tanvd.aorm.withDatabase

class ViewClickhouseTest : AormTestBase() {

    @Test
    fun createView_viewExists() {
        withDatabase(database) {
            ExampleTable.create()
            ExampleView.create()
            Assertions.assertTrue(ExampleView.exists())
        }
    }

    @Test
    fun dropView_viewDoesNotExist() {
        withDatabase(database) {
            ExampleTable.create()
            ExampleView.create()
            ExampleView.drop()
            Assertions.assertFalse(ExampleView.exists())
        }
    }


    @Test
    fun insert_viewExistsRowValid_rowInserted() {
        withDatabase(database) {
            ExampleTable.create()
            ExampleView.create()

            val row = prepareInsertRow(mapOf(ExampleTable.id to 2L, ExampleTable.value to "value",
                    ExampleTable.date to getDate("2000-01-01"), ExampleTable.arrayValue to listOf("array1", "array2"))
                    .toMutableMap())
            InsertClickhouse.insert(database, InsertExpression(ExampleTable, ExampleTable.columns, arrayListOf(row)))


            val result = ExampleView.select(ExampleView.idView, ExampleView.valueView).toResult()

            Assertions.assertEquals(result.single(), prepareSelectRow(mapOf(ExampleView.idView to 2L, ExampleView.valueView to "value")))
        }
    }


}
