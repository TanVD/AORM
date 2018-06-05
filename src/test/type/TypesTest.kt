package type

import org.testng.Assert
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import tanvd.aorm.*
import tanvd.aorm.expression.Column
import tanvd.aorm.implementation.InsertClickhouse
import tanvd.aorm.implementation.MetadataClickhouse
import tanvd.aorm.implementation.QueryClickhouse
import tanvd.aorm.query.and
import tanvd.aorm.query.eq
import tanvd.aorm.query.where
import utils.TestDatabase
import utils.getDate
import utils.getDateTime

@Suppress("UNCHECKED_CAST")
class TypesTest {

    @BeforeMethod
    fun resetTable() {
        withDatabase(TestDatabase) {
            try {
                AllTypesTable.drop()
            } catch (e: Exception) {
            }
        }
    }

    @Test
    fun allTypes_createTable_tableCreated() {
        withDatabase(TestDatabase) {
            AllTypesTable.create()

            Assert.assertTrue(MetadataClickhouse.existsTable(TestDatabase, AllTypesTable))
        }
    }

    @Test
    fun allTypes_insertIntoTable_rowInserted() {
        withDatabase(TestDatabase) {

            AllTypesTable.create()

            val row = Row(mapOf(
                    AllTypesTable.dateCol to getDate("2000-01-01"),
                    AllTypesTable.dateTimeCol to getDateTime("2000-01-01 12:00:00"),
                    AllTypesTable.ulongCol to 1L,
                    AllTypesTable.longCol to 2L,
                    AllTypesTable.intCol to 3,
                    AllTypesTable.boolCol to true,
                    AllTypesTable.stringCol to "string",
                    AllTypesTable.arrayDateCol to listOf(getDate("2001-01-01"), getDate("2002-02-02")),
//                AllTypesTable.arrayDateTimeCol to listOf(getDateTime("2001-01-01 11:11:11"), getDateTime("2002-02-02 12:12:12")),
                    AllTypesTable.arrayUlongCol to listOf(3L, 4L),
                    AllTypesTable.arrayLongCol to listOf(5L, 6L),
                    AllTypesTable.arrayBoolCol to listOf(true, false),
                    AllTypesTable.arrayStringCol to listOf("string1", "string2")
            ).toMutableMap() as MutableMap<Column<Any, DbType<Any>>, Any>)

            InsertClickhouse.insert(TestDatabase, InsertExpression(AllTypesTable, AllTypesTable.columns, arrayListOf(row)))

            val gotRow = (AllTypesTable.select() where ((AllTypesTable.dateCol eq getDate("2000-01-01")) and
                    (AllTypesTable.dateTimeCol eq getDateTime("2000-01-01 12:00:00")) and
                    (AllTypesTable.ulongCol eq 1L) and
                    (AllTypesTable.longCol eq 2L) and
                    (AllTypesTable.intCol eq 3) and
                    (AllTypesTable.boolCol eq true) and
                    (AllTypesTable.stringCol eq "string"))).toResult().single()

            Assert.assertEquals(gotRow, row)
        }
    }

    @Test
    fun allTypes_insertIntoTable_insertSqlValid() {
        withDatabase(TestDatabase) {

            AllTypesTable.create()

            val row = Row(mapOf(
                    AllTypesTable.dateCol to getDate("2000-01-01"),
                    AllTypesTable.dateTimeCol to getDateTime("2000-01-01 12:00:00"),
                    AllTypesTable.ulongCol to 1L,
                    AllTypesTable.longCol to 2L,
                    AllTypesTable.intCol to 3,
                    AllTypesTable.boolCol to true,
                    AllTypesTable.stringCol to "string",
                    AllTypesTable.arrayDateCol to listOf(getDate("2001-01-01"), getDate("2002-02-02")),
//                AllTypesTable.arrayDateTimeCol to listOf(getDateTime("2001-01-01 11:11:11"), getDateTime("2002-02-02 12:12:12")),
                    AllTypesTable.arrayUlongCol to listOf(3L, 4L),
                    AllTypesTable.arrayLongCol to listOf(5L, 6L),
                    AllTypesTable.arrayBoolCol to listOf(true, false),
                    AllTypesTable.arrayStringCol to listOf("string1", "string2")
            ).toMutableMap() as MutableMap<Column<Any, DbType<Any>>, Any>)

            val sql = InsertClickhouse.constructInsert(InsertExpression(AllTypesTable, AllTypesTable.columns, arrayListOf(row)))
            Assert.assertEquals(sql, "INSERT INTO all_types_table (date_col, datetime_col, ulong_col, long_col, int_col, bool_col, string_col, arrayDate_col, arrayULong_col, arrayLong_col, arrayBoolean_col, arrayString_col) VALUES ('2000-01-01', '2000-01-01 12:00:00', 1, 2, 3, 1, 'string', ['2001-01-01', '2002-02-02'], [3, 4], [5, 6], [1, 0], ['string1', 'string2']);")
        }
    }

    @Test
    fun allTypes_queryTable_querySqlValid() {
        withDatabase(TestDatabase) {

            AllTypesTable.create()

            val query = (AllTypesTable.select() where ((AllTypesTable.dateCol eq getDate("2000-01-01")) and
                    (AllTypesTable.dateTimeCol eq getDateTime("2000-01-01 12:00:00")) and
                    (AllTypesTable.ulongCol eq 1L) and
                    (AllTypesTable.longCol eq 2L) and
                    (AllTypesTable.intCol eq 3) and
                    (AllTypesTable.boolCol eq true) and
                    (AllTypesTable.stringCol eq "string")))

            val sql = QueryClickhouse.constructQuery(query)
            Assert.assertEquals(sql, "SELECT date_col, datetime_col, ulong_col, long_col, int_col, bool_col, string_col, arrayDate_col, arrayULong_col, arrayLong_col, arrayBoolean_col, arrayString_col FROM all_types_table WHERE (((((((date_col = '2000-01-01') AND (datetime_col = '2000-01-01 12:00:00')) AND (ulong_col = 1)) AND (long_col = 2)) AND (int_col = 3)) AND (bool_col = 1)) AND (string_col = 'string')) ;")
        }
    }
}

object AllTypesTable : Table("all_types_table") {

    val dateCol = date("date_col")
    val dateTimeCol = datetime("datetime_col")
    val ulongCol = uint64("ulong_col")
    val longCol = int64("long_col")
    val intCol = int32("int_col")
    val boolCol = boolean("bool_col")
    val stringCol = string("string_col")

    val arrayDateCol = arrayDate("arrayDate_col")
    //    val arrayDateTimeCol = arrayDateTime("arrayDateTime_col")
    val arrayUlongCol = arrayUInt64("arrayULong_col")
    val arrayLongCol = arrayInt64("arrayLong_col")
    val arrayBoolCol = arrayBoolean("arrayBoolean_col")
    val arrayStringCol = arrayString("arrayString_col")

    override val engine: Engine = Engine.MergeTree(dateCol, listOf(dateCol))
}