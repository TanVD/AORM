package utils

import org.testng.annotations.BeforeMethod
import tanvd.aorm.DbType
import tanvd.aorm.InsertRow
import tanvd.aorm.SelectRow
import tanvd.aorm.expression.Column
import tanvd.aorm.expression.Expression

abstract class AormTestBase {
    @BeforeMethod
    fun resetDb() {
        ignoringExceptions {
            ExampleTable.resetTable()
        }
        executeBeforeMethod()
    }

    open fun executeBeforeMethod() {}

    @Suppress("UNCHECKED_CAST")
    fun prepareInsertRow(map: Map<Column<*, DbType<*>>, Any>): InsertRow {
        return InsertRow(map.toMutableMap() as MutableMap<Column<Any, DbType<Any>>, Any>)
    }

    @Suppress("UNCHECKED_CAST")
    fun prepareSelectRow(map: Map<Column<*, DbType<*>>, Any>): SelectRow {
        return SelectRow(map.toMutableMap() as MutableMap<Expression<Any, DbType<Any>>, Any>)
    }
}

fun ignoringExceptions(body: () -> Unit) {
    try {
        body()
    } catch (e: Exception) {
    }
}