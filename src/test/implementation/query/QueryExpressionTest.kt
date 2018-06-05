package implementation.query

import org.testng.Assert
import org.testng.annotations.Test
import tanvd.aorm.expression.count
import tanvd.aorm.implementation.QueryClickhouse
import tanvd.aorm.withDatabase
import utils.ExampleTable
import utils.TestDatabase

class QueryExpressionTest {
    @Test
    fun count_countByColumn_sqlValid() {
        withDatabase(TestDatabase) {
            val query = ExampleTable.select(count(ExampleTable.id))

            val sql = QueryClickhouse.constructQuery(query)
            Assert.assertEquals(sql, "SELECT COUNT(${ExampleTable.id.name}) FROM ExampleTable ;")
        }
    }
}