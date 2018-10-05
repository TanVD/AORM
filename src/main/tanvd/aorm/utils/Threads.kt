package tanvd.aorm.utils

import org.slf4j.Logger
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread


fun ScheduledExecutorService?.withExceptionLogger(logger: Logger) = ExecutorServiceWrapperWithExceptionLogger(this!!, logger)

class ExecutorServiceWrapperWithExceptionLogger(val delegate: ScheduledExecutorService, val logger: Logger) : ScheduledExecutorService by delegate {
    override fun schedule(command: Runnable?, delay: Long, unit: TimeUnit?): ScheduledFuture<*> = delegate.schedule(wrapRunnable(command), delay, unit)

    override fun scheduleAtFixedRate(command: Runnable?, initialDelay: Long, period: Long, unit: TimeUnit?): ScheduledFuture<*> = delegate.scheduleAtFixedRate(wrapRunnable(command), initialDelay, period, unit)

    override fun scheduleWithFixedDelay(command: Runnable?, initialDelay: Long, delay: Long, unit: TimeUnit?): ScheduledFuture<*> = delegate.scheduleWithFixedDelay(wrapRunnable(command), initialDelay, delay, unit)

    private fun wrapRunnable(command: Runnable?) = Runnable {
        try {
            command?.run()
        } catch (e: Throwable) {
            logger.error("Uncaught exception from thread ${Thread.currentThread().name}", e)
        }
    }
}

fun namedFactory(threadNamePrefix: String, isDaemon: Boolean = false) = object : ThreadFactory {
    private val num = AtomicInteger(1)
    override fun newThread(r: Runnable) = thread(start = false, isDaemon = isDaemon, name = "$threadNamePrefix-${num.getAndIncrement()}") {
        r.run()
    }
}

fun ExecutorService.shutdownNowGracefully(logger: Logger, timeout: Long = 10000) = executeAndAwait(timeout, logger, ExecutorService::shutdownNow)

private fun <T> ExecutorService.executeAndAwait(timeout: Long, logger: Logger, exec: ExecutorService.() -> T) {
    exec(this)
    awaitTermination(timeout, TimeUnit.MILLISECONDS)
    if (!isTerminated) {
        logger.warn("ExecutorService ${this} was asked to shutdown gracefully, but was not stopped in $timeout ms.")
        println(Thread.currentThread().stackTrace.joinToString("\n\t") { it.toString() })
    }
}
