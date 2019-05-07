package tanvd.aorm.implementation.query

import org.testng.Assert
import org.testng.annotations.Test
import tanvd.aorm.implementation.QueryClickhouse
import tanvd.aorm.query.*
import tanvd.aorm.utils.AormTestBase
import tanvd.aorm.utils.ExampleTable
import tanvd.aorm.withDatabase

class QueryArrayConditionTest : AormTestBase() {
    @Test
    fun exists_stringArray_sqlValid() {
        withDatabase(database) {
            val expression = (ExampleTable.arrayValue exists { x -> x eq "value" })
            val query = ExampleTable.select() where expression

            val sql = QueryClickhouse.constructQuery(query)
            Assert.assertEquals(sql, "SELECT ${ExampleTable.columns.joinToString { it.name }} FROM ExampleTable" +
                    " WHERE (arrayExists(x -> (x = 'value'), string_array)) ;")
        }
    }

    @Test
    fun match_stringArray_sqlValid() {
        withDatabase(database) {
            val expression = (ExampleTable.arrayValue has "value")
            val query = ExampleTable.select() where expression

            val sql = QueryClickhouse.constructQuery(query)
            Assert.assertEquals(sql, "SELECT ${ExampleTable.columns.joinToString { it.name }} FROM ExampleTable" +
                    " WHERE (has(string_array, 'value')) ;")
        }
    }

}
