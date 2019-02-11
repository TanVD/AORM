package tanvd.aorm.implementation.query

import org.testng.Assert
import org.testng.annotations.Test
import tanvd.aorm.implementation.QueryClickhouse
import tanvd.aorm.query.eq
import tanvd.aorm.query.exists
import tanvd.aorm.query.has
import tanvd.aorm.query.where
import tanvd.aorm.utils.ExampleTable
import tanvd.aorm.utils.TestDatabase
import tanvd.aorm.withDatabase

class QueryArrayConditionTest {
    @Test
    fun exists_stringArray_sqlValid() {
        withDatabase(TestDatabase) {
            val expression = (ExampleTable.arrayValue exists { x -> x eq "value" })
            val query = ExampleTable.select() where expression

            val sql = QueryClickhouse.constructQuery(query)
            Assert.assertEquals(sql, "SELECT ${ExampleTable.columns.joinToString { it.name }} FROM ExampleTable" +
                    " WHERE (arrayExists(x -> (x = 'value'), string_array)) ;")
        }
    }

    @Test
    fun match_stringArray_sqlValid() {
        withDatabase(TestDatabase) {
            val expression = (ExampleTable.arrayValue has "value")
            val query = ExampleTable.select() where expression

            val sql = QueryClickhouse.constructQuery(query)
            Assert.assertEquals(sql, "SELECT ${ExampleTable.columns.joinToString { it.name }} FROM ExampleTable" +
                    " WHERE (has(string_array, 'value')) ;")
        }
    }

}
