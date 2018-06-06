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
import tanvd.aorm.query.has
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
                    AllTypesTable.arrayDateCol to listOf(getDate("2001-01-01"), getDate("2002-02-02")),

                    AllTypesTable.dateTimeCol to getDateTime("2000-01-01 12:00:00"),
                    //                AllTypesTable.arrayDateTimeCol to listOf(getDateTime("2001-01-01 11:11:11"), getDateTime("2002-02-02 12:12:12")),

                    AllTypesTable.int8Col to 1.toByte(),
                    AllTypesTable.arrayInt8Col to listOf(1.toByte(), 2.toByte()),
                    AllTypesTable.uint8Col to 1.toByte(),
                    AllTypesTable.arrayUInt8Col to listOf(1.toByte(), 2.toByte()),

                    AllTypesTable.int16Col to 1.toShort(),
                    AllTypesTable.arrayInt16Col to listOf(1.toShort(), 2.toShort()),
                    AllTypesTable.uint16Col to 1.toShort(),
                    AllTypesTable.arrayUInt16Col to listOf(1.toShort(), 2.toShort()),

                    AllTypesTable.int32Col to 1,
                    AllTypesTable.arrayInt32Col to listOf(1, 2),
                    AllTypesTable.uint32Col to 1,
                    AllTypesTable.arrayUInt32Col to listOf(1, 2),

                    AllTypesTable.int64Col to 1L,
                    AllTypesTable.arrayInt64Col to listOf(1L, 2L),
                    AllTypesTable.uint64Col to 1L,
                    AllTypesTable.arrayUInt64Col to listOf(1L, 2L),

                    AllTypesTable.enum8Col to TestEnumFirst.first_enum,
                    AllTypesTable.enum16Col to TestEnumFirst.second_enum,

                    AllTypesTable.boolCol to true,
                    AllTypesTable.arrayBoolCol to listOf(true, false),

                    AllTypesTable.stringCol to "string",
                    AllTypesTable.arrayStringCol to listOf("string1", "string2")
            ).toMutableMap() as MutableMap<Column<Any, DbType<Any>>, Any>)

            InsertClickhouse.insert(TestDatabase, InsertExpression(AllTypesTable, AllTypesTable.columns, arrayListOf(row)))

            val gotRow = (AllTypesTable.select() where ((AllTypesTable.dateCol eq getDate("2000-01-01")) and
                    //TODO-tanvd date has not working in JDBC
//                    (AllTypesTable.arrayDateCol has getDate("2001-01-01")) and
//                    (AllTypesTable.arrayDateCol has getDate("2002-02-02")) and

                    (AllTypesTable.dateTimeCol eq getDateTime("2000-01-01 12:00:00")) and
                    
                    (AllTypesTable.int8Col eq 1.toByte()) and
                    (AllTypesTable.arrayInt8Col has 1.toByte()) and
                    (AllTypesTable.arrayInt8Col has 2.toByte()) and
                    (AllTypesTable.uint8Col eq 1.toByte()) and
                    (AllTypesTable.arrayUInt8Col has 1.toByte()) and
                    (AllTypesTable.arrayUInt8Col has 2.toByte()) and

                    (AllTypesTable.int16Col eq 1.toShort()) and
                    (AllTypesTable.arrayInt16Col has 1.toShort()) and
                    (AllTypesTable.arrayInt16Col has 2.toShort()) and
                    (AllTypesTable.uint16Col eq 1.toShort()) and
                    (AllTypesTable.arrayUInt16Col has 1.toShort()) and
                    (AllTypesTable.arrayUInt16Col has 2.toShort()) and

                    (AllTypesTable.int32Col eq 1) and
                    (AllTypesTable.arrayInt32Col has 1) and
                    (AllTypesTable.arrayInt32Col has 2) and
                    (AllTypesTable.uint32Col eq 1) and
                    (AllTypesTable.arrayUInt32Col has 1) and
                    (AllTypesTable.arrayUInt32Col has 2) and

                    (AllTypesTable.int64Col eq 1L) and
                    (AllTypesTable.arrayInt64Col has 1L) and
                    (AllTypesTable.arrayInt64Col has 2L) and
                    (AllTypesTable.uint64Col eq 1L) and
                    (AllTypesTable.arrayUInt64Col has 1L) and
                    (AllTypesTable.arrayUInt64Col has 2L) and

                    (AllTypesTable.enum8Col eq TestEnumFirst.first_enum) and
                    (AllTypesTable.enum16Col eq TestEnumFirst.second_enum) and
                    
                    (AllTypesTable.boolCol eq true) and
                    (AllTypesTable.arrayBoolCol has true) and
                    (AllTypesTable.arrayBoolCol has false) and

                    (AllTypesTable.stringCol eq "string") and
                    (AllTypesTable.arrayStringCol has "string1") and
                    (AllTypesTable.arrayStringCol has "string2")
                    )).toResult().single()

            Assert.assertEquals(gotRow, row)
        }
    }

    @Test
    fun allTypes_insertIntoTable_insertSqlValid() {
        withDatabase(TestDatabase) {

            AllTypesTable.create()

            val row = Row(mapOf(
                    AllTypesTable.dateCol to getDate("2000-01-01"),
                    AllTypesTable.arrayDateCol to listOf(getDate("2001-01-01"), getDate("2002-02-02")),

                    AllTypesTable.dateTimeCol to getDateTime("2000-01-01 12:00:00"),
                    //                AllTypesTable.arrayDateTimeCol to listOf(getDateTime("2001-01-01 11:11:11"), getDateTime("2002-02-02 12:12:12")),

                    AllTypesTable.int8Col to 1.toByte(),
                    AllTypesTable.arrayInt8Col to listOf(1.toByte(), 2.toByte()),
                    AllTypesTable.uint8Col to 1.toByte(),
                    AllTypesTable.arrayUInt8Col to listOf(1.toByte(), 2.toByte()),

                    AllTypesTable.int16Col to 1.toShort(),
                    AllTypesTable.arrayInt16Col to listOf(1.toShort(), 2.toShort()),
                    AllTypesTable.uint16Col to 1.toShort(),
                    AllTypesTable.arrayUInt16Col to listOf(1.toShort(), 2.toShort()),

                    AllTypesTable.int32Col to 1,
                    AllTypesTable.arrayInt32Col to listOf(1, 2),
                    AllTypesTable.uint32Col to 1,
                    AllTypesTable.arrayUInt32Col to listOf(1, 2),

                    AllTypesTable.int64Col to 1L,
                    AllTypesTable.arrayInt64Col to listOf(1L, 2L),
                    AllTypesTable.uint64Col to 1L,
                    AllTypesTable.arrayUInt64Col to listOf(1L, 2L),

                    AllTypesTable.enum8Col to TestEnumFirst.first_enum,
                    AllTypesTable.enum16Col to TestEnumFirst.second_enum,

                    AllTypesTable.boolCol to true,
                    AllTypesTable.arrayBoolCol to listOf(true, false),

                    AllTypesTable.stringCol to "string",
                    AllTypesTable.arrayStringCol to listOf("string1", "string2")
            ).toMutableMap() as MutableMap<Column<Any, DbType<Any>>, Any>)


            val sql = InsertClickhouse.constructInsert(InsertExpression(AllTypesTable, AllTypesTable.columns, arrayListOf(row)))
            Assert.assertEquals(sql, "INSERT INTO all_types_table (date_col, arrayDate_col, datetime_col, enum8_col, enum16_col, int8_col, arrayInt8_col, uint8_col, arrayUInt8_col, int16_col, arrayInt16_col, uint16_col, arrayUInt16_col, int32_col, arrayInt32_col, uint32_col, arrayUInt32_col, int64_col, arrayInt64_col, uint64_col, arrayUInt64_col, bool_col, arrayBoolean_col, string_col, arrayString_col) VALUES ('2000-01-01', ['2001-01-01', '2002-02-02'], '2000-01-01 12:00:00', first_enum, second_enum, 1, [1, 2], 1, [1, 2], 1, [1, 2], 1, [1, 2], 1, [1, 2], 1, [1, 2], 1, [1, 2], 1, [1, 2], 1, [1, 0], 'string', ['string1', 'string2']);")
        }
    }

    @Test
    fun allTypes_queryTable_querySqlValid() {
        withDatabase(TestDatabase) {

            AllTypesTable.create()

            val query = AllTypesTable.select() where ((AllTypesTable.dateCol eq getDate("2000-01-01")) and
                    (AllTypesTable.arrayDateCol has getDate("2001-01-01")) and
                    (AllTypesTable.arrayDateCol has getDate("2002-02-02")) and

                    (AllTypesTable.dateTimeCol eq getDateTime("2000-01-01 12:00:00")) and

                    (AllTypesTable.int8Col eq 1.toByte()) and
                    (AllTypesTable.arrayInt8Col has 1.toByte()) and
                    (AllTypesTable.arrayInt8Col has 2.toByte()) and
                    (AllTypesTable.uint8Col eq 1.toByte()) and
                    (AllTypesTable.arrayUInt8Col has 1.toByte()) and
                    (AllTypesTable.arrayUInt8Col has 2.toByte()) and

                    (AllTypesTable.int16Col eq 1.toShort()) and
                    (AllTypesTable.arrayInt16Col has 1.toShort()) and
                    (AllTypesTable.arrayInt16Col has 2.toShort()) and
                    (AllTypesTable.uint16Col eq 1.toShort()) and
                    (AllTypesTable.arrayUInt16Col has 1.toShort()) and
                    (AllTypesTable.arrayUInt16Col has 2.toShort()) and

                    (AllTypesTable.int32Col eq 1) and
                    (AllTypesTable.arrayInt32Col has 1) and
                    (AllTypesTable.arrayInt32Col has 2) and
                    (AllTypesTable.uint32Col eq 1) and
                    (AllTypesTable.arrayUInt32Col has 1) and
                    (AllTypesTable.arrayUInt32Col has 2) and

                    (AllTypesTable.int64Col eq 1L) and
                    (AllTypesTable.arrayInt64Col has 1L) and
                    (AllTypesTable.arrayInt64Col has 2L) and
                    (AllTypesTable.uint64Col eq 1L) and
                    (AllTypesTable.arrayUInt64Col has 1L) and
                    (AllTypesTable.arrayUInt64Col has 2L) and

                    (AllTypesTable.enum8Col eq TestEnumFirst.first_enum) and
                    (AllTypesTable.enum16Col eq TestEnumFirst.second_enum) and

                    (AllTypesTable.boolCol eq true) and
                    (AllTypesTable.arrayBoolCol has true) and
                    (AllTypesTable.arrayBoolCol has false) and

                    (AllTypesTable.stringCol eq "string") and
                    (AllTypesTable.arrayStringCol has "string1") and
                    (AllTypesTable.arrayStringCol has "string2"))

            val sql = QueryClickhouse.constructQuery(query)
            Assert.assertEquals(sql, "SELECT date_col, arrayDate_col, datetime_col, enum8_col, enum16_col, int8_col, arrayInt8_col, uint8_col, arrayUInt8_col, int16_col, arrayInt16_col, uint16_col, arrayUInt16_col, int32_col, arrayInt32_col, uint32_col, arrayUInt32_col, int64_col, arrayInt64_col, uint64_col, arrayUInt64_col, bool_col, arrayBoolean_col, string_col, arrayString_col FROM all_types_table WHERE ((((((((((((((((((((((((((((((((((((date_col = '2000-01-01') AND (has(arrayDate_col, '2001-01-01'))) AND (has(arrayDate_col, '2002-02-02'))) AND (datetime_col = '2000-01-01 12:00:00')) AND (int8_col = 1)) AND (has(arrayInt8_col, 1))) AND (has(arrayInt8_col, 2))) AND (uint8_col = 1)) AND (has(arrayUInt8_col, 1))) AND (has(arrayUInt8_col, 2))) AND (int16_col = 1)) AND (has(arrayInt16_col, 1))) AND (has(arrayInt16_col, 2))) AND (uint16_col = 1)) AND (has(arrayUInt16_col, 1))) AND (has(arrayUInt16_col, 2))) AND (int32_col = 1)) AND (has(arrayInt32_col, 1))) AND (has(arrayInt32_col, 2))) AND (uint32_col = 1)) AND (has(arrayUInt32_col, 1))) AND (has(arrayUInt32_col, 2))) AND (int64_col = 1)) AND (has(arrayInt64_col, 1))) AND (has(arrayInt64_col, 2))) AND (uint64_col = 1)) AND (has(arrayUInt64_col, 1))) AND (has(arrayUInt64_col, 2))) AND (enum8_col = first_enum)) AND (enum16_col = second_enum)) AND (bool_col = 1)) AND (has(arrayBoolean_col, 1))) AND (has(arrayBoolean_col, 0))) AND (string_col = 'string')) AND (has(arrayString_col, 'string1'))) AND (has(arrayString_col, 'string2'))) ;")
        }
    }
}

enum class TestEnumFirst {
    first_enum,
    second_enum
}

object AllTypesTable : Table("all_types_table") {

    val dateCol = date("date_col")
    val arrayDateCol = arrayDate("arrayDate_col")

    val dateTimeCol = datetime("datetime_col")
    //    val arrayDateTimeCol = arrayDateTime("arrayDateTime_col")

    val enum8Col = enum8("enum8_col", TestEnumFirst::class)
    val enum16Col = enum16("enum16_col", TestEnumFirst::class)

    val int8Col = int8("int8_col")
    val arrayInt8Col = arrayInt8("arrayInt8_col")
    val uint8Col = uint8("uint8_col")
    val arrayUInt8Col = arrayUInt8("arrayUInt8_col")

    val int16Col = int16("int16_col")
    val arrayInt16Col = arrayInt16("arrayInt16_col")
    val uint16Col = uint16("uint16_col")
    val arrayUInt16Col = arrayUInt16("arrayUInt16_col")

    val int32Col = int32("int32_col")
    val arrayInt32Col = arrayInt32("arrayInt32_col")
    val uint32Col = uint32("uint32_col")
    val arrayUInt32Col = arrayUInt32("arrayUInt32_col")

    val int64Col = int64("int64_col")
    val arrayInt64Col = arrayInt64("arrayInt64_col")
    val uint64Col = uint64("uint64_col")
    val arrayUInt64Col = arrayUInt64("arrayUInt64_col")

    val boolCol = boolean("bool_col")
    val arrayBoolCol = arrayBoolean("arrayBoolean_col")

    val stringCol = string("string_col")
    val arrayStringCol = arrayString("arrayString_col")

    override val engine: Engine = Engine.MergeTree(dateCol, listOf(dateCol))
}