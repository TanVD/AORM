package tanvd.aorm.utils

import com.clickhouse.data.ClickHouseUtils

internal inline fun <T : AutoCloseable?, R> T.use(block: (T) -> R): R {
    var exception: Throwable? = null
    try {
        return block(this)
    } catch (e: Throwable) {
        exception = e
        throw e
    } finally {
        when {
            this == null -> {
            }
            exception == null -> close()
            else ->
                try {
                    close()
                } catch (closeException: Throwable) {
                    exception.addSuppressed(closeException)
                }
        }
    }
}

private fun String.escape(char: Char): String = ClickHouseUtils.escape(this, char)

/**
The list of characters should be in sync with [ClickHouseUtils.isQuote]
 */
internal fun String.escapeQuotes(): String = escape('"').escape('\'').escape('`')