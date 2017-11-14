package tanvd.aorm

import org.apache.commons.dbcp2.DataSourceConnectionFactory
import org.apache.commons.dbcp2.PoolableConnectionFactory
import org.apache.commons.dbcp2.PoolingDataSource
import org.apache.commons.pool2.impl.GenericObjectPool
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import ru.yandex.clickhouse.ClickHouseDataSource
import ru.yandex.clickhouse.settings.ClickHouseProperties
import tanvd.aorm.exceptions.BasicDbException
import java.io.Closeable
import java.sql.Connection

abstract class DatabaseProperties {
    abstract val name: String

    abstract val url: String
    abstract val password: String
    abstract val user: String

    abstract val useSsl: Boolean
    abstract val sslCertPath: String
    abstract val sslVerifyMode: String

    open val maxTotalHttpThreads = 1000
    open val maxPerRouteHttpThreads = 500

    open val socketTimeout = 30000
    open val connectionTimeout = 10000
    open val keepAliveTimeout = 30000
    open val timeToLiveMillis = 60000
    open val dataTransferTimeout = 20000

    open val maxTotal = 60
    open val maxIdle = 30
    open val minIdle = 1
    open val testOnBorrow = true
    open val testWhileIdle = true
    open val timeBetweenEvictionRunsMillis = 30000L
}

abstract class Database(val properties: DatabaseProperties) : Closeable {

    open val objectPoolConfig by lazy {
        val config = GenericObjectPoolConfig()
        config.testOnBorrow = properties.testOnBorrow
        config.testWhileIdle = properties.testWhileIdle
        config.timeBetweenEvictionRunsMillis = properties.timeBetweenEvictionRunsMillis
        config.maxTotal = properties.maxTotal
        config.maxIdle = properties.maxIdle
        config.minIdle = properties.minIdle
        config
    }


    protected val pooledDataSource by lazy {
        val chProperties = ClickHouseProperties()
        chProperties.user = properties.user
        chProperties.password = properties.password

        chProperties.ssl = properties.useSsl
        chProperties.sslRootCertificate = properties.sslCertPath
        chProperties.sslMode = properties.sslVerifyMode

        chProperties.maxTotal = properties.maxTotalHttpThreads
        chProperties.defaultMaxPerRoute = properties.maxPerRouteHttpThreads
        chProperties.socketTimeout = properties.socketTimeout
        chProperties.connectionTimeout = properties.connectionTimeout
        chProperties.keepAliveTimeout = properties.keepAliveTimeout
        chProperties.timeToLiveMillis = properties.timeToLiveMillis
        chProperties.dataTransferTimeout = properties.dataTransferTimeout
        val dataSource = ClickHouseDataSource(properties.url, chProperties)
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

    override fun close() {
        pooledDataSource.close()
    }
}

