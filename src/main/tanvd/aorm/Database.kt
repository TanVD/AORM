package tanvd.aorm

import org.apache.commons.dbcp2.DataSourceConnectionFactory
import org.apache.commons.dbcp2.PoolableConnectionFactory
import org.apache.commons.dbcp2.PoolingDataSource
import org.apache.commons.pool2.impl.GenericObjectPool
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import ru.yandex.clickhouse.ClickHouseDataSource
import ru.yandex.clickhouse.settings.ClickHouseProperties
import tanvd.aorm.exceptions.BasicDbException
import java.sql.Connection

abstract class Database {
    abstract val name: String

    abstract fun url(): String
    abstract fun password(): String
    abstract fun user(): String

    abstract fun useSsl(): Boolean
    abstract fun sslCertPath(): String
    abstract fun sslVerifyMode(): String

    open fun maxTotalHttpThreads() = 1000
    open fun maxPerRouteHttpThreads() = 500

    open fun socketTimeout() = 30000
    open fun connectionTimeout() = 10000
    open fun keepAliveTimeout() = 30000
    open fun timeToLiveMillis() = 60000
    open fun dataTransferTimeout() = 20000

    open val objectPoolConfig by lazy {
        val config = GenericObjectPoolConfig()
        config.testOnBorrow = testOnBorrow()
        config.testWhileIdle = testWhileIdle()
        config.timeBetweenEvictionRunsMillis = timeBetweenEvictionRunsMillis()
        config.maxTotal = maxTotal()
        config.maxIdle = maxIdle()
        config.minIdle = minIdle()
        config
    }

    open fun maxTotal() = 60
    open fun maxIdle() = 30
    open fun minIdle() = 1
    open fun testOnBorrow() = true
    open fun testWhileIdle() = true
    open fun timeBetweenEvictionRunsMillis() = 30000L

    protected val pooledDataSource by lazy {
        val properties = ClickHouseProperties()
        properties.user = user()
        properties.password = password()
        properties.maxTotal = maxTotalHttpThreads()
        properties.defaultMaxPerRoute = maxPerRouteHttpThreads()
        properties.socketTimeout = socketTimeout()
        properties.connectionTimeout = connectionTimeout()
        properties.keepAliveTimeout = keepAliveTimeout()
        properties.timeToLiveMillis = timeToLiveMillis()
        properties.dataTransferTimeout = dataTransferTimeout()
        val dataSource = ClickHouseDataSource(url(), properties)
        val connectionFactory = DataSourceConnectionFactory(dataSource)
        val poolableConnectionFactory = PoolableConnectionFactory(connectionFactory, null)
        val connectionPool = GenericObjectPool(poolableConnectionFactory, objectPoolConfig)
        poolableConnectionFactory.pool = connectionPool
        connectionPool.preparePool()
        PoolingDataSource(connectionPool)
    }

    fun <T> withConnection(body: Connection.() -> T): T {
        return try {
            pooledDataSource.connection.use {
                it.body()
            }
        } catch (e: Exception) {
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