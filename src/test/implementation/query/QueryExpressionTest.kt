package implementation.query

import org.testng.Assert
import org.testng.annotations.Test
import tanvd.aorm.expression.count
import tanvd.aorm.implementation.QueryClickhouse
import utils.ExampleTable

class QueryExpressionTest {
    @Test
    fun count_countByColumn_sqlValid() {
        val query = ExampleTable.select(count(ExampleTable.id))

        val sql = QueryClickhouse.constructQuery(query)
        Assert.assertEquals(sql, "SELECT COUNT(${ExampleTable.id.name}) FROM ExampleTable ;")
    }
}