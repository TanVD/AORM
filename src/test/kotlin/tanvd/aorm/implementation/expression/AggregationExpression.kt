package tanvd.aorm.implementation.expression

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import tanvd.aorm.expression.max
import tanvd.aorm.implementation.InsertClickhouse
import tanvd.aorm.insert.InsertExpression
import tanvd.aorm.utils.AormTestBase
import tanvd.aorm.utils.ExampleTable
import tanvd.aorm.utils.getDate
import tanvd.aorm.withDatabase

class AggregationExpression : AormTestBase() {
    @Test
    fun divide_function() {
        withDatabase(database) {
            ExampleTable.create()
            val rows = arrayListOf(prepareInsertRow(
                    mapOf(ExampleTable.id to 100L, ExampleTable.value to "value", ExampleTable.date to getDate("2000-01-01"))
                ), prepareInsertRow(
                    mapOf(ExampleTable.id to 1000L, ExampleTable.value to "value", ExampleTable.date to getDate("2100-01-01"))
                ))

            InsertClickhouse.insert(database, InsertExpression(ExampleTable, ExampleTable.columns, rows))
            val maxId = max(ExampleTable.id)
            val maxDate = max(ExampleTable.date)
            val result = ExampleTable.select(maxId, maxDate).toResult().single()
            Assertions.assertEquals(1000L, result[maxId])
            Assertions.assertEquals(getDate("2100-01-01"), result[maxDate])
        }
    }
}