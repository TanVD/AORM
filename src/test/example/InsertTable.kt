//package example
//
//import org.testng.annotations.AfterMethod
//import org.testng.annotations.BeforeClass
//import org.testng.annotations.Test
//import tanvd.aorm.model.insert
//import tanvd.aorm.model.insertWithColumns
//import tanvd.aorm.model.values
//
//class AormTableInsertExample {
//
//    @BeforeClass
//    fun createTable() {
//        AuditTable.create()
//    }
//
//    @AfterMethod
//    fun dropTable() {
////        AuditTable.drop()
//    }
//
//    @Test
//    fun example_insert() {
//        AuditTable insert listOf(AuditTable.version to 1L)
//    }
//
//    @Test
//    fun example_insert_batch() {
//        AuditTable insertWithColumns listOf(AuditTable.version, AuditTable.id) values listOf(
//                listOf(AuditTable.version to 1L, AuditTable.id to 2L),
//                listOf(AuditTable.version to 5L)
//        )
//    }
//}