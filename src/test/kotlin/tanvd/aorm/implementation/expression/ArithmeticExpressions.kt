package tanvd.aorm.implementation.expression

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import tanvd.aorm.expression.div
import tanvd.aorm.implementation.InsertClickhouse
import tanvd.aorm.insert.InsertExpression
import tanvd.aorm.utils.AormTestBase
import tanvd.aorm.utils.ExampleTable
import tanvd.aorm.utils.getDate
import tanvd.aorm.withDatabase

class ArithmeticExpressions : AormTestBase() {
    @Test
    fun divide_function() {
        withDatabase(database) {
            ExampleTable.create()
            val row = prepareInsertRow(mapOf(ExampleTable.id to 100L, ExampleTable.value to "value",
                    ExampleTable.date to getDate("2000-01-01"))
                    .toMutableMap())

            InsertClickhouse.insert(database, InsertExpression(ExampleTable, ExampleTable.columns, arrayListOf(row)))

            val divideExpression = ExampleTable.id / ExampleTable.id
            val select = ExampleTable.select(divideExpression).toResult()
            Assertions.assertEquals(1.0, select.single()[divideExpression])
        }
    }
}