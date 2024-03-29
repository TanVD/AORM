package tanvd.aorm.implementation.query

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import tanvd.aorm.expression.*
import tanvd.aorm.implementation.QueryClickhouse
import tanvd.aorm.query.groupBy
import tanvd.aorm.utils.AormTestBase
import tanvd.aorm.utils.ExampleTable
import tanvd.aorm.withDatabase

class QueryExpressionTest : AormTestBase() {
    @Test
    fun count_countByColumn_sqlValid() {
        withDatabase(database) {
            val query = ExampleTable.select(count(ExampleTable.id))

            val sql = QueryClickhouse.constructQuery(query)
            Assertions.assertEquals(sql, "SELECT COUNT(${ExampleTable.id.name}) FROM ExampleTable")
        }
    }

    @Test
    fun maxState_maxStateByColumn_sqlValid() {
        withDatabase(database) {
            val query = ExampleTable.select(maxState(ExampleTable.id)).groupBy(ExampleTable.id)

            val sql = QueryClickhouse.constructQuery(query)
            Assertions.assertEquals(sql, "SELECT maxState(${ExampleTable.id.name}) FROM ExampleTable GROUP BY id")
        }
    }


    @Test
    fun maxMerge_maxMergeByColumn_sqlValid() {
        withDatabase(database) {
            val query = ExampleTable.select(maxMerge(maxState(ExampleTable.id))).groupBy(ExampleTable.id)

            val sql = QueryClickhouse.constructQuery(query)
            Assertions.assertEquals(sql, "SELECT maxMerge(maxState(${ExampleTable.id.name})) FROM ExampleTable GROUP BY id")
        }
    }

    @Test
    fun argMaxState_maxStateByColumn_sqlValid() {
        withDatabase(database) {
            val query = ExampleTable.select(argMaxState(ExampleTable.id, ExampleTable.value)).groupBy(ExampleTable.id)

            val sql = QueryClickhouse.constructQuery(query)
            Assertions.assertEquals(sql, "SELECT argMaxState(${ExampleTable.id.name}, ${ExampleTable.value.name}) FROM ExampleTable GROUP BY id")
        }
    }


    @Test
    fun argMaxMerge_maxMergeByColumn_sqlValid() {
        withDatabase(database) {
            val query = ExampleTable.select(argMaxMerge(argMaxState(ExampleTable.id, ExampleTable.value))).groupBy(ExampleTable.id)

            val sql = QueryClickhouse.constructQuery(query)
            Assertions.assertEquals(sql, "SELECT argMaxMerge(argMaxState(${ExampleTable.id.name}, ${ExampleTable.value.name})) FROM ExampleTable GROUP BY id")
        }
    }
}
