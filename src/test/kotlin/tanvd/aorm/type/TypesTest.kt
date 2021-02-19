package tanvd.aorm.type

import org.junit.jupiter.api.*
import tanvd.aorm.*
import tanvd.aorm.implementation.*
import tanvd.aorm.insert.InsertExpression
import tanvd.aorm.query.*
import tanvd.aorm.utils.*
import java.math.BigDecimal

@Suppress("UNCHECKED_CAST")
class TypesTest : AormTestBase() {

    @BeforeEach
    fun resetTable() {
        withDatabase(database) {
            try {
                AllTypesTable.drop()
            } catch (e: Exception) {
            }
        }
    }

    @Test
    fun allTypes_createTable_tableCreated() {
        withDatabase(database) {
            AllTypesTable.create()

            Assertions.assertTrue(MetadataClickhouse.existsTable(database, AllTypesTable))
        }
    }

    @Test
    fun allTypes_insertIntoTable_rowInserted() {
        withDatabase(database) {

            AllTypesTable.create()

            val row = prepareInsertRow(mapOf(
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

                    AllTypesTable.float32Col to 1f,
                    AllTypesTable.arrayFloat32Col to listOf(1f, 2f),
                    AllTypesTable.float64Col to 1.0,
                    AllTypesTable.arrayFloat64Col to listOf(1.0, 2.0),

                    AllTypesTable.decimalCol to BigDecimal("1234.56"),
                    AllTypesTable.arrayDecimalCol to arrayListOf(BigDecimal("1234.56"), BigDecimal("78.9")),

                    AllTypesTable.enum8Col to TestEnumFirst.first_enum,
                    AllTypesTable.enum16Col to TestEnumFirst.second_enum,

                    AllTypesTable.boolCol to true,
                    AllTypesTable.arrayBoolCol to listOf(true, false),

                    AllTypesTable.stringCol to "string",
                    AllTypesTable.arrayStringCol to listOf("string1", "string2")
            ).toMutableMap())

            InsertClickhouse.insert(database, InsertExpression(AllTypesTable, AllTypesTable.columns, arrayListOf(row)))

            // we need to disable query parser stack limitation for tests, so we add line "<max_parser_depth>0</max_parser_depth>" to the config:
            serverContainer.execInContainer("sed", "-i", "/^.*\\<max_memory_usage\\>.*/a \\<max_parser_depth\\>0\\<\\/max_parser_depth\\>", "/etc/clickhouse-server/users.xml")
            Thread.sleep(1000)  // wait for server to reload just changed config

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

                    (AllTypesTable.float32Col eq 1f) and
                    (AllTypesTable.arrayFloat32Col has 1f) and
                    (AllTypesTable.arrayFloat32Col has 2f) and

                    (AllTypesTable.float64Col eq 1.0) and
                    (AllTypesTable.arrayFloat64Col has 1.0) and
                    (AllTypesTable.arrayFloat64Col has 2.0) and

                    (AllTypesTable.decimalCol eq BigDecimal("1234.56")) and
                    (AllTypesTable.arrayDecimalCol has BigDecimal("1234.56")) and
                    (AllTypesTable.arrayDecimalCol has BigDecimal("78.9")) and

                    (AllTypesTable.enum8Col eq TestEnumFirst.first_enum) and
                    (AllTypesTable.enum16Col eq TestEnumFirst.second_enum) and

                    (AllTypesTable.boolCol eq true) and
                    (AllTypesTable.arrayBoolCol has true) and
                    (AllTypesTable.arrayBoolCol has false) and

                    (AllTypesTable.stringCol eq "string") and
                    (AllTypesTable.arrayStringCol has "string1") and
                    (AllTypesTable.arrayStringCol has "string2")
                    )).toResult().single()

            AssertDb.assertEquals(gotRow, row)
        }
    }

    @Test
    fun allTypes_insertIntoTable_insertSqlValid() {
        withDatabase(database) {

            AllTypesTable.create()

            val row = prepareInsertRow(mapOf(
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

                    AllTypesTable.float32Col to 1f,
                    AllTypesTable.arrayFloat32Col to listOf(1f, 2f),
                    AllTypesTable.float64Col to 1.0,
                    AllTypesTable.arrayFloat64Col to listOf(1.0, 2.0),

                    AllTypesTable.decimalCol to BigDecimal("1234.56"),
                    AllTypesTable.arrayDecimalCol to listOf(BigDecimal("1234.56"), BigDecimal("78.90")),

                    AllTypesTable.enum8Col to TestEnumFirst.first_enum,
                    AllTypesTable.enum16Col to TestEnumFirst.second_enum,

                    AllTypesTable.boolCol to true,
                    AllTypesTable.arrayBoolCol to listOf(true, false),

                    AllTypesTable.stringCol to "string",
                    AllTypesTable.arrayStringCol to listOf("string1", "string2")
            ).toMutableMap())


            val sql = InsertClickhouse.constructInsert(InsertExpression(AllTypesTable, AllTypesTable.columns, arrayListOf(row)))
            Assertions.assertEquals("INSERT INTO all_types_table (date_col, arrayDate_col, datetime_col, enum8_col, enum16_col, int8_col, arrayInt8_col, uint8_col, arrayUInt8_col, int16_col, arrayInt16_col, uint16_col, arrayUInt16_col, int32_col, arrayInt32_col, uint32_col, arrayUInt32_col, int64_col, arrayInt64_col, uint64_col, arrayUInt64_col, float32_col, arrayFloat32_col, float64_col, arrayFloat64_col, decimal_col, arrayDecimal_col, bool_col, arrayBoolean_col, string_col, arrayString_col) VALUES ('2000-01-01', ['2001-01-01', '2002-02-02'], '2000-01-01 12:00:00', first_enum, second_enum, 1, [1, 2], 1, [1, 2], 1, [1, 2], 1, [1, 2], 1, [1, 2], 1, [1, 2], 1, [1, 2], 1, [1, 2], 1.0, [1.0, 2.0], 1.0, [1.0, 2.0], 1234.56, [1234.56, 78.90], 1, [1, 0], 'string', ['string1', 'string2']);", sql)
        }
    }

    @Test
    fun allTypes_queryTable_querySqlValid() {
        withDatabase(database) {

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

                    (AllTypesTable.float32Col eq 1f) and
                    (AllTypesTable.arrayFloat32Col has 1f) and
                    (AllTypesTable.arrayFloat32Col has 2f) and

                    (AllTypesTable.float64Col eq 1.0) and
                    (AllTypesTable.arrayFloat64Col has 1.0) and
                    (AllTypesTable.arrayFloat64Col has 2.0) and

                    (AllTypesTable.decimalCol eq BigDecimal("1234.56")) and
                    (AllTypesTable.arrayDecimalCol has BigDecimal("1234.56")) and
                    (AllTypesTable.arrayDecimalCol has BigDecimal("78.9")) and

                    (AllTypesTable.enum8Col eq TestEnumFirst.first_enum) and
                    (AllTypesTable.enum16Col eq TestEnumFirst.second_enum) and

                    (AllTypesTable.boolCol eq true) and
                    (AllTypesTable.arrayBoolCol has true) and
                    (AllTypesTable.arrayBoolCol has false) and

                    (AllTypesTable.stringCol eq "string") and
                    (AllTypesTable.arrayStringCol has "string1") and
                    (AllTypesTable.arrayStringCol has "string2"))

            val sql = QueryClickhouse.constructQuery(query)
            Assertions.assertEquals("SELECT date_col, arrayDate_col, datetime_col, enum8_col, enum16_col, int8_col, arrayInt8_col, uint8_col, arrayUInt8_col, int16_col, arrayInt16_col, uint16_col, arrayUInt16_col, int32_col, arrayInt32_col, uint32_col, arrayUInt32_col, int64_col, arrayInt64_col, uint64_col, arrayUInt64_col, float32_col, arrayFloat32_col, float64_col, arrayFloat64_col, decimal_col, arrayDecimal_col, bool_col, arrayBoolean_col, string_col, arrayString_col FROM all_types_table WHERE (((((((((((((((((((((((((((((((((((((((((((((date_col = '2000-01-01') AND (has(arrayDate_col, '2001-01-01'))) AND (has(arrayDate_col, '2002-02-02'))) AND (datetime_col = '2000-01-01 12:00:00')) AND (int8_col = 1)) AND (has(arrayInt8_col, 1))) AND (has(arrayInt8_col, 2))) AND (uint8_col = 1)) AND (has(arrayUInt8_col, 1))) AND (has(arrayUInt8_col, 2))) AND (int16_col = 1)) AND (has(arrayInt16_col, 1))) AND (has(arrayInt16_col, 2))) AND (uint16_col = 1)) AND (has(arrayUInt16_col, 1))) AND (has(arrayUInt16_col, 2))) AND (int32_col = 1)) AND (has(arrayInt32_col, 1))) AND (has(arrayInt32_col, 2))) AND (uint32_col = 1)) AND (has(arrayUInt32_col, 1))) AND (has(arrayUInt32_col, 2))) AND (int64_col = 1)) AND (has(arrayInt64_col, 1))) AND (has(arrayInt64_col, 2))) AND (uint64_col = 1)) AND (has(arrayUInt64_col, 1))) AND (has(arrayUInt64_col, 2))) AND (float32_col = 1.0)) AND (has(arrayFloat32_col, 1.0))) AND (has(arrayFloat32_col, 2.0))) AND (float64_col = 1.0)) AND (has(arrayFloat64_col, 1.0))) AND (has(arrayFloat64_col, 2.0))) AND (decimal_col = toDecimal64(1234.56, 2))) AND (has(arrayDecimal_col, toDecimal64(1234.56, 2)))) AND (has(arrayDecimal_col, toDecimal64(78.9, 2)))) AND (enum8_col = first_enum)) AND (enum16_col = second_enum)) AND (bool_col = 1)) AND (has(arrayBoolean_col, 1))) AND (has(arrayBoolean_col, 0))) AND (string_col = 'string')) AND (has(arrayString_col, 'string1'))) AND (has(arrayString_col, 'string2'))) ;", sql)
        }
    }
}

@Suppress("EnumEntryName")
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

    val float32Col = float32("float32_col")
    val arrayFloat32Col = arrayFloat32("arrayFloat32_col")
    val float64Col = float64("float64_col")
    val arrayFloat64Col = arrayFloat64("arrayFloat64_col")

    val decimalCol = decimal64("decimal_col", 2)
    val arrayDecimalCol = arrayDecimal64("arrayDecimal_col", 2)

    val boolCol = boolean("bool_col")
    val arrayBoolCol = arrayBoolean("arrayBoolean_col")

    val stringCol = string("string_col")
    val arrayStringCol = arrayString("arrayString_col")

    override val engine: Engine = Engine.MergeTree(dateCol, listOf(dateCol))
}
