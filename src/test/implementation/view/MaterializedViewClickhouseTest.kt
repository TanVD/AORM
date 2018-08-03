package implementation.view

import org.junit.Assert
import org.testng.annotations.Test
import tanvd.aorm.InsertExpression
import tanvd.aorm.implementation.InsertClickhouse
import tanvd.aorm.withDatabase
import utils.*

class MaterializedViewClickhouseTest : AormTestBase() {

    @Test
    fun createView_viewExists() {
        withDatabase(TestDatabase) {
            ExampleTable.create()
            ExampleMaterializedView.create()
            Assert.assertTrue(ExampleMaterializedView.exists())
        }
    }

    @Test
    fun dropView_viewDoesNotExist() {
        withDatabase(TestDatabase) {
            ExampleTable.create()
            ExampleMaterializedView.create()
            ExampleMaterializedView.drop()
            Assert.assertFalse(ExampleMaterializedView.exists())
        }
    }


    @Test
    fun insert_viewExistsRowValid_rowInserted() {
        withDatabase(TestDatabase) {
            ExampleTable.create()
            ExampleMaterializedView.create()

            val row = prepareInsertRow(mapOf(ExampleTable.id to 2L, ExampleTable.value to "value",
                    ExampleTable.date to getDate("2000-01-01"), ExampleTable.arrayValue to listOf("array1", "array2"))
                    .toMutableMap())
            InsertClickhouse.insert(TestDatabase, InsertExpression(ExampleTable, ExampleTable.columns, arrayListOf(row)))


            val result = ExampleMaterializedView.select(ExampleMaterializedView.date.alias, ExampleMaterializedView.idView.alias, ExampleMaterializedView.valueView.alias).toResult()

            Assert.assertEquals(result.single(), prepareSelectRow(mapOf(ExampleMaterializedView.date.alias to getDate("2000-01-01"),
                    ExampleMaterializedView.idView.alias to 2L, ExampleMaterializedView.valueView.alias to "value")))
        }
    }


}