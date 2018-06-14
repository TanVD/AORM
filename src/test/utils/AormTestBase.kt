package utils

import org.testng.annotations.BeforeMethod
import tanvd.aorm.DbType
import tanvd.aorm.InsertRow
import tanvd.aorm.SelectRow
import tanvd.aorm.expression.Column

abstract class AormTestBase {
    @BeforeMethod
    fun resetDb() {
        ignoringExceptions {
            ExampleTable.resetTable()
        }
        executeBeforeMethod()
    }

    open fun executeBeforeMethod() {}

    fun prepareInsertRow(map: Map<Column<*, DbType<*>>, Any>): InsertRow {
        return InsertRow(map.toMutableMap())
    }

    fun prepareSelectRow(map: Map<Column<*, DbType<*>>, Any>): SelectRow {
        return SelectRow(map.toMutableMap())
    }
}

fun ignoringExceptions(body: () -> Unit) {
    try {
        body()
    } catch (e: Exception) {
    }
}