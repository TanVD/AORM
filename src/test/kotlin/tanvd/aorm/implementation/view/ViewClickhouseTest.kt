package tanvd.aorm.implementation.view

import org.junit.Assert
import org.testng.annotations.Test
import tanvd.aorm.implementation.InsertClickhouse
import tanvd.aorm.insert.InsertExpression
import tanvd.aorm.utils.*
import tanvd.aorm.withDatabase

class ViewClickhouseTest : AormTestBase() {

    @Test
    fun createView_viewExists() {
        withDatabase(TestDatabase) {
            ExampleTable.create()
            ExampleView.create()
            Assert.assertTrue(ExampleView.exists())
        }
    }

    @Test
    fun dropView_viewDoesNotExist() {
        withDatabase(TestDatabase) {
            ExampleTable.create()
            ExampleView.create()
            ExampleView.drop()
            Assert.assertFalse(ExampleView.exists())
        }
    }


    @Test
    fun insert_viewExistsRowValid_rowInserted() {
        withDatabase(TestDatabase) {
            ExampleTable.create()
            ExampleView.create()

            val row = prepareInsertRow(mapOf(ExampleTable.id to 2L, ExampleTable.value to "value",
                    ExampleTable.date to getDate("2000-01-01"), ExampleTable.arrayValue to listOf("array1", "array2"))
                    .toMutableMap())
            InsertClickhouse.insert(TestDatabase, InsertExpression(ExampleTable, ExampleTable.columns, arrayListOf(row)))


            val result = ExampleView.select(ExampleView.idView, ExampleView.valueView).toResult()

            Assert.assertEquals(result.single(), prepareSelectRow(mapOf(ExampleView.idView to 2L, ExampleView.valueView to "value")))
        }
    }


}
