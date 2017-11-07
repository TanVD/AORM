package utils

import tanvd.aorm.Database

object TestDatabase : Database(){
    override val name: String = "default"

    override val url: String = /*System.getProperty("ClickhouseUrl")?.trim('"') ?: */"jdbc:clickhouse://localhost:8123"
    override val password: String = ""
    override val user: String = "default"

    override val useSsl: Boolean = false
    override val sslCertPath: String = ""
    override val sslVerifyMode: String = ""

}
