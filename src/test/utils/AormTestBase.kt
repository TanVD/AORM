package utils

import org.testng.annotations.BeforeMethod

abstract class AormTestBase {
    @BeforeMethod
    fun resetDb() {
        try {
            ExampleTable.resetTable()
        } catch (e : Exception) {}
    }
}