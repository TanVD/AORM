package utils

import ru.yandex.clickhouse.ClickHouseDataSource
import ru.yandex.clickhouse.settings.ClickHouseProperties
import tanvd.aorm.Database

object TestDatabase : Database("default",
        ClickHouseDataSource(System.getProperty("ClickhouseUrl")?.trim('"') ?: "jdbc:clickhouse://localhost:8123",
        ClickHouseProperties().apply {
            user = "default"
            password = ""
        }))
