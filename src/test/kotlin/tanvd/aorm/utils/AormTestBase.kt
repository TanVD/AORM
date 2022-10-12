package tanvd.aorm.utils

import com.clickhouse.client.config.ClickHouseDefaults
import com.clickhouse.jdbc.ClickHouseDataSource
import org.junit.jupiter.api.BeforeEach
import org.testcontainers.containers.ClickHouseContainer
import tanvd.aorm.*
import tanvd.aorm.expression.Column
import tanvd.aorm.expression.Expression
import tanvd.aorm.insert.DefaultInsertWorker
import java.util.*

abstract class AormTestBase {
    companion object {
        const val testInsertWorkerDelayMs = 2000L

        val serverContainer = object : ClickHouseContainer("clickhouse/clickhouse-server:22.3.8.39-alpine") {
            override fun getDriverClassName(): String = "com.clickhouse.jdbc.ClickHouseDriver"
        }.apply {
            start()
            // we need to disable query parser stack limitation for tests, so we add line "<max_parser_depth>0</max_parser_depth>" to the config:
            execInContainer("sed", "-i", "/^.*\\<max_memory_usage\\>.*/a \\<max_parser_depth\\>0\\<\\/max_parser_depth\\>", "/etc/clickhouse-server/users.xml")
            Thread.sleep(5_000)   // wait for server to reload just changed config
        }
    }

    val database by lazy {
        Database(
            name = "default",
            dataSource = ClickHouseDataSource(serverContainer.jdbcUrl, Properties().apply {
                put(ClickHouseDefaults.USER, serverContainer.username)
                put(ClickHouseDefaults.PASSWORD, serverContainer.password)
            })
        )
    }

    val insertWorker by lazy {
        DefaultInsertWorker("test-insert-worker", delayTimeMs = testInsertWorkerDelayMs, betweenCallsTimeMs = testInsertWorkerDelayMs)
    }

    @BeforeEach
    fun resetDb() {
        tryRun {
            ExampleTable.resetTable(database)
        }
        withDatabase(database) {
            tryRun {
                ExampleView.drop()
            }
            tryRun {
                ExampleMaterializedView.drop()
            }
        }
        executeBeforeMethod()
    }

    open fun executeBeforeMethod() {}

    fun prepareInsertRow(map: Map<Column<*, DbType<*>>, Any>): InsertRow {
        return InsertRow(map.toMutableMap())
    }

    fun prepareSelectRow(map: Map<Expression<*, DbType<*>>, Any>): SelectRow {
        return SelectRow(map.toMutableMap())
    }
}

fun tryRun(body: () -> Unit) = try {
    body()
} catch (_: Throwable) {
}
