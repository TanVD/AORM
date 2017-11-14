package utils

import org.testng.annotations.BeforeMethod

abstract class AormTestBase {
    @BeforeMethod
    fun resetDb() {
        ignoringExceptions {
            ExampleTable.resetTable()
        }
        executeBeforeMethod()
    }

    open fun executeBeforeMethod() {

    }
}

fun ignoringExceptions(body: () -> Unit) {
    try {
        body()
    } catch (e: Exception) {}
}