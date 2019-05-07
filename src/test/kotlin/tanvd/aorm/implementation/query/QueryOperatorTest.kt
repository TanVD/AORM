package tanvd.aorm.implementation.query

import org.testng.Assert
import org.testng.annotations.Test
import tanvd.aorm.implementation.QueryClickhouse
import tanvd.aorm.query.*
import tanvd.aorm.utils.AormTestBase
import tanvd.aorm.utils.ExampleTable
import tanvd.aorm.withDatabase

class QueryOperatorTest : AormTestBase() {
    @Test
    fun and_twoConditions_validSQL() {
        withDatabase(database) {
            val expression = (ExampleTable.id eq 1L) and (ExampleTable.value eq "string")
            val query = ExampleTable.select() where expression

            val sql = QueryClickhouse.constructQuery(query)
            Assert.assertEquals(sql, "SELECT ${ExampleTable.columns.joinToString { it.name }} FROM" +
                    " ExampleTable WHERE ((id = 1) AND (value = 'string')) ;")
        }
    }

    @Test
    fun or_twoConditions_validSQL() {
        withDatabase(database) {
            val expression = (ExampleTable.id eq 1L) or (ExampleTable.value eq "string")
            val query = ExampleTable.select() where expression

            val sql = QueryClickhouse.constructQuery(query)
            Assert.assertEquals(sql, "SELECT ${ExampleTable.columns.joinToString { it.name }} FROM" +
                    " ExampleTable WHERE ((id = 1) OR (value = 'string')) ;")
        }
    }

    @Test
    fun not_oneCondition_validSQL() {
        withDatabase(database) {
            val expression = not(ExampleTable.id eq 1L)
            val query = ExampleTable.select() where expression

            val sql = QueryClickhouse.constructQuery(query)
            Assert.assertEquals(sql, "SELECT ${ExampleTable.columns.joinToString { it.name }} FROM" +
                    " ExampleTable WHERE (NOT (id = 1)) ;")
        }
    }
}
