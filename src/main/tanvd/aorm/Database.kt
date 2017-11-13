package tanvd.aorm

import ru.yandex.clickhouse.ClickHouseDataSource
import ru.yandex.clickhouse.settings.ClickHouseProperties
import tanvd.aorm.exceptions.BasicDbException
import java.sql.Connection
import javax.sql.DataSource

abstract class Database {
    abstract val name: String

    abstract val url: String
    abstract val password: String
    abstract val user: String

    abstract val useSsl: Boolean
    abstract val sslCertPath: String
    abstract val sslVerifyMode: String

    open val maxTotal = 1000
    open val maxPerRoute = 500

    open val socketTimeout = 30000
    open val connectionTimeout = 10000

    open val keepAliveTimeout = 30000
    open val timeToLiveMillis = 60000

    open val dataTransferTimeout = 20000


    private val dataSource: DataSource by lazy {
        val properties = ClickHouseProperties()
        properties.user = user
        properties.password = password
        properties.maxTotal = maxTotal
        properties.defaultMaxPerRoute = maxPerRoute
        properties.socketTimeout = socketTimeout
        properties.connectionTimeout = connectionTimeout
        properties.keepAliveTimeout = keepAliveTimeout
        properties.timeToLiveMillis = timeToLiveMillis
        properties.dataTransferTimeout = dataTransferTimeout
        ClickHouseDataSource(url, properties)
    }

    fun <T>withConnection(body: Connection.() -> T) : T {
        return try {
            dataSource.connection.use {
                it.body()
            }
        } catch (e : Exception) {
            throw BasicDbException("Exception occurred executing DB call", e)
        }
    }

    fun execute(sql: String) {
        withConnection {
            prepareStatement(sql).use {
                it.execute()
            }
        }
    }
}