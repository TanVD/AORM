//package example
//
//import org.joda.time.DateTime
//import org.testng.annotations.Test
//import tanvd.aorm.*
//import utils.TestDatabase
//import java.util.*
//
//class AormTableCreateExample {
//    @Test
//    fun example_create() {
//        AuditTable.syncScheme()
//    }
//
//    @Test
//    fun example_drop() {
//        AuditTable.drop()
//    }
//
//    @Test
//    fun example_alter_column() {
//        AuditTable.create()
//        AuditTable.addColumn(Column("date_2_column", DbDate()))
//    }
//
//    @Test
//    fun example_drop_column() {
//        AuditTable.create()
//        AuditTable.dropColumn(AuditTable.version)
//    }
//}
//
//object AuditTable : Table("Audit") {
//    override var db: Database = TestDatabase
//
//    val date = date("date_column").default { DateTime.now().toDate() }
//    val id = long("id_column").default { Random().nextLong() }
//    val version = ulong("version_column")
//
//    val another_id = arrayString("another_id")
//
//    override val engine = MergeTree(date, listOf(date, id), 8192)
//}