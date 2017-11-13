package utils

import tanvd.aorm.Database

object TestDatabase : Database() {
    override val name: String = "default"

    override fun url(): String = System.getProperty("ClickhouseUrl")?.trim('"') ?: "jdbc:clickhouse://localhost:8123"
    override fun password(): String = ""
    override fun user(): String = "default"

    override fun useSsl(): Boolean = false
    override fun sslCertPath(): String = ""
    override fun sslVerifyMode(): String = ""
}
