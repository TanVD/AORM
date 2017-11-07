//package example
//
//import org.testng.annotations.AfterMethod
//import org.testng.annotations.BeforeClass
//import org.testng.annotations.Test
//import tanvd.aorm.model.query.eq
//import tanvd.aorm.model.query.or
//import tanvd.aorm.model.query.where
//
//class AormTableSelectExample {
//
//    @BeforeClass
//    fun createTable() {
////        AuditTable.create()
//    }
//
//    @AfterMethod
//    fun dropTable() {
////        AuditTable.drop()
//    }
//
//    @Test
//    fun example_select() {
//        val query = AuditTable.select() where ((AuditTable.id eq 0) or (AuditTable.id eq 2))
//        val rows = query.toResult()
//        println(rows.size)
//    }
//}