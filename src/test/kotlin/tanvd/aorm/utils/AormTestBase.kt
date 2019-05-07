package tanvd.aorm.utils

import org.junit.jupiter.api.BeforeEach
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import ru.yandex.clickhouse.ClickHouseDataSource
import ru.yandex.clickhouse.settings.ClickHouseProperties
import tanvd.aorm.*
import tanvd.aorm.expression.Column
import tanvd.aorm.expression.Expression
import tanvd.aorm.insert.DefaultInsertWorker

@Testcontainers
abstract class AormTestBase {
    companion object {
        const val testInsertWorkerDelayMs = 2000L
        private const val containerPort = 8123

        @Container
        private val localstack = KGenericContainer("yandex/clickhouse-server:19.5")
                .withExposedPorts(containerPort)
                .apply { start() }
    }

    val database by lazy {
        Database("default", ClickHouseDataSource("jdbc:clickhouse://localhost:${localstack.getMappedPort(containerPort)}",
                ClickHouseProperties().withCredentials("default", "")))
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
} catch (e: Throwable) {
}
