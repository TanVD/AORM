package utils

import tanvd.aorm.Database
import tanvd.aorm.DatabaseProperties

object TestDatabaseProperties : DatabaseProperties() {
    override val name: String = "default"

    override val url: String = System.getProperty("ClickhouseUrl")?.trim('"') ?: "jdbc:clickhouse://localhost:8123"
    override val password: String = ""
    override val user: String = "default"

    override val useSsl: Boolean = false
    override val sslCertPath: String = ""
    override val sslVerifyMode: String = ""
}

object TestDatabase : Database(TestDatabaseProperties)
