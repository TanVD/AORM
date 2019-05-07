# AORM

[![CircleCI](https://circleci.com/gh/TanVD/AORM.svg?style=svg)](https://circleci.com/gh/TanVD/AORM)
[ ![Download](https://api.bintray.com/packages/tanvd/aorm/aorm/images/download.svg) ](https://bintray.com/tanvd/aorm/aorm/_latestVersion)

AORM is analytical SQL framework. Basically, it is a fork of [Exposed SQL framework](https://github.com/JetBrains/Exposed) for ClickHouse database dialect.

AORM supports Exposed-like DSL for standard SQL operations and rich set of ClickHouse dialect features (like state-aggregation functions, different engines, replicated context of execution and so on)


## Setup

AORM releases are published to [JCenter](https://bintray.com/tanvd/aorm/aorm).

## How to

First of all you'll need to set up Database object. Provide it with DataSource (self-constructed, JNDI -- it doesn't matter, but don't forget to use pooling :) ). In context of Database (`withContext(db)` call) you will perform all operations.

```kotlin
val database = Database("default",
        ClickHouseDataSource("jdbc:clickhouse://localhost:8123",
                ClickHouseProperties().withCredentials("default", ""))
)
```

If you have a replicated cluster and want to balance load you may need to set up few Database objects and use ReplicatedConnectionContext.

You can set up Table objects once database is created. Right now AORM supports a lot of ClickHouse types, but does not support nullability. Instead, null values will fallback to ClickHouse defaults. Support of nullability is considered to be implemented.

```kotlin
object TestTable : Table("test_table") {
     val dateCol = date("date_col")
     val int8Col = int8("int8_col")
     val int64Col = int64("int64_col")
     val stringCol = string("string_col")
     val arrayStringCol = arrayString("arrayString_col")
 
     override val engine: Engine = Engine.MergeTree(dateCol, listOf(dateCol))
}
 ```

Please note, that table is not linked to specific database. Table object is only declaration of scheme. You can use it in different contexts during work with different databases.

Once you have created table object, you can align the scheme of your table with you database. Aligning of scheme means, that table will be created if it does not exist, or, if it exists, not existing columns will be added. AORM, by default, not performing any removal operations on aligning of scheme. It will not drop existing tables or columns.

```kotlin
withDatabase(database) {
    TestTable.syncScheme() // this call will align the scheme of TestTable in a database
}
```

Once everything is set up, you can insert some data:

```kotlin
withDatabase(database) {
    TestTable.insert {
        it[TestTable.dateCol] = SimpleDateFormat("yyyy-MM-dd").parse("2000-01-01")
        it[TestTable.int8Col] = 1.toByte()
        it[TestTable.int64Col] = 1L
        it[TestTable.stringCol] = "test"
        it[TestTable.arrayStringCol] = listOf("test1", "test2")
    }
}
```

Then load it:

```kotlin
withDatabase(database) {
    val query = TestTable.select() where (TestTable.int8Col eq 1.toByte())
    val res = query.toResult()
}
```

There are a lot more query functions, types of columns, and so on. You can take a closer look at all features of AORM at it's tests or at next section


## More examples

A lot of examples of AORM production usage are located at [JetAudit library](https://github.com/TanVD/JetAudit). JetAudit is library for reliable and fast saving of business processes events (audit-events) and for reading of such events. It uses ClickHouse as data warehouse and AORM is used as programmatic interface.
