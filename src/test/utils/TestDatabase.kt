package utils

import ru.yandex.clickhouse.ClickHouseDataSource
import ru.yandex.clickhouse.settings.ClickHouseProperties
import tanvd.aorm.Database

val TestDatabase = Database("default",
        ClickHouseDataSource(System.getProperty("clickhouseUrl") ?: "jdbc:clickhouse://localhost:8123",
                ClickHouseProperties().apply {
                    user = System.getProperty("clickhouseUser")?.takeIf(String::isNotBlank) ?: "default"
                    password = System.getProperty("clickhousePassword")?.takeIf(String::isNotBlank)
                }))
