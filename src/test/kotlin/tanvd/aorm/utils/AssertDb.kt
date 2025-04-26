package tanvd.aorm.utils

import org.junit.jupiter.api.Assertions
import tanvd.aorm.*
import tanvd.aorm.expression.Column
import tanvd.aorm.implementation.MetadataClickhouse
import java.math.BigDecimal
import java.sql.Date

object AssertDb {
    fun syncedWithDb(database: Database, table: Table) {
        Assertions.assertTrue(MetadataClickhouse.existsTable(database, table))

        val metadataColumns = MetadataClickhouse.columnsOfTable(database, table)

        for ((name, type) in metadataColumns) {
            Assertions.assertTrue(table.columns.any { column -> column.name == name && column.type.toSqlName() == type })
        }

        for (column in table.columns) {
            Assertions.assertTrue(metadataColumns.any { (name, type) -> name == column.name && type == column.type.toSqlName() })
        }
    }

    fun assertEquals(selectRow: SelectRow, insertRow: InsertRow) {
        deepMapEqualsWithBigDecimalSupport(insertRow.values, selectRow.values)
    }

    fun assertEquals(selectRow: Set<SelectRow>, insertRow: Set<InsertRow>) {
        Assertions.assertEquals(insertRow.map { it.values }.toSet(), selectRow.map { it.values }.toSet())
    }

    fun assertEquals(selectRow: List<SelectRow>, insertRow: List<InsertRow>) {
        Assertions.assertEquals(insertRow.map { it.values }, selectRow.map { it.values })
    }

    private fun deepMapEqualsWithBigDecimalSupport(expected: Map<Column<*,*>, *>, actual: Map<*, *>) {
        Assertions.assertEquals(expected.size, actual.size) {
            "Size of maps is different: ${expected.size} != ${actual.size}"
        }

        expected.forEach { (k, v1) ->
            val v2 = actual[k]
            Assertions.assertNotNull(v2) { "Expected value for key $k absent from actual map"}
            when (v1) {
                is Collection<*> -> {
                    val v2AsCollection = v2 as? Collection<*>
                    Assertions.assertNotNull(v2AsCollection) { "Actual value is ${v2!!::class.simpleName} type while ${v1::class.simpleName} expected for ${k.name}" }
                    Assertions.assertEquals(v1.size, v2AsCollection!!.size)
                    val element = v1.firstOrNull()
                    when (element) {
                        is BigDecimal -> {
                            v1.zip(v2).forEach { (expected, actual) ->
                                assertBigDecimalEquals(expected, actual)
                            }
                        }
                        is Date -> {
                            v1.zip(v2).forEach { (expected, actual) ->
                                Assertions.assertEquals("$expected", "$actual", "Values for key ${k.name} are different")
                            }
                        }

                        else -> {
                            Assertions.assertEquals(v1, v2, "Collection elements are different for key ${k.name}")
                        }
                    }
                }
                !is BigDecimal -> {
                    Assertions.assertEquals(v1, v2, "Values for key ${k.name} are different")
                }
                else -> {
                    assertBigDecimalEquals(v1, v2)
                }
            }
        }
    }

    private fun assertBigDecimalEquals(expected: Any?, actual: Any?) {
        val actualAsBigDecimal = actual as? BigDecimal
        val expectedAsBigDecimal = actual as? BigDecimal
        Assertions.assertNotNull(actualAsBigDecimal) { "Actual value is ${actual!!::class.simpleName} type while BigDecimal expected" }
        Assertions.assertNotNull(expectedAsBigDecimal) { "Expected value is ${expected!!::class.simpleName} type while BigDecimal expected" }
        Assertions.assertTrue(actualAsBigDecimal!!.compareTo(expectedAsBigDecimal) == 0)
    }
}
