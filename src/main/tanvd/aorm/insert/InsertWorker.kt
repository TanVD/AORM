package tanvd.aorm.insert

import org.slf4j.LoggerFactory
import tanvd.aorm.Database
import tanvd.aorm.implementation.InsertClickhouse
import tanvd.aorm.utils.namedFactory
import tanvd.aorm.utils.shutdownNowGracefully
import tanvd.aorm.utils.withExceptionLogger
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

interface InsertWorker {
    fun add(db: Database, insertExpression: InsertExpression)

    fun stop()
}

class DefaultInsertWorker(val name: String, queueSizeMax: Int = 10000,
                          delayTimeMs: Long = 30 * 1000,
                          initalDelayTimeMs: Long = 30 * 1000,
                          betweenCallsTimeMs: Long = 10 * 1000) : InsertWorker {

    private val logger = LoggerFactory.getLogger(DefaultInsertWorker::class.java)

    private var isWorking = AtomicBoolean(true)

    private val insertWorker = Executors.newSingleThreadScheduledExecutor(namedFactory(name)).withExceptionLogger(logger)
    private val assetUsageQueue = ArrayBlockingQueue<Pair<Database, InsertExpression>>(queueSizeMax)

    init {
        insertWorker.scheduleWithFixedDelay({
            val data = arrayListOf<Pair<Database, InsertExpression>>().apply { assetUsageQueue.drainTo(this) }
            val preparedData = data.groupBy { it.first }.mapValues { it.value.map { it.second }.groupBy { it.table }.toMutableMap() }.toMutableMap()
            while (preparedData.isNotEmpty()) {
                for (db in preparedData.keys) {
                    val tables = preparedData[db]!!
                    val table = tables.keys.first()
                    val inserts = tables[table]!!

                    val columns = table.columnsWithDefaults.toMutableSet()
                    columns += inserts.flatMap { it.columns }
                    val batchedInsertExpression = InsertExpression(table, columns, inserts.flatMap { it.values }.toMutableList())
                    InsertClickhouse.insert(db, batchedInsertExpression)

                    tables.remove(table)

                    if (tables.isEmpty()) {
                        preparedData.remove(db)
                    }
                }
                Thread.sleep(betweenCallsTimeMs)
            }
        }, initalDelayTimeMs, delayTimeMs, TimeUnit.MILLISECONDS)
    }

    override fun add(db: Database, insertExpression: InsertExpression) {
        if (isWorking.get()) {
            assetUsageQueue.add(db to insertExpression)
        }
    }

    override fun stop() {
        isWorking.set(false)
        insertWorker.shutdownNowGracefully(logger)
    }

}