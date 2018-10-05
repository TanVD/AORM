package tanvd.aorm

import tanvd.aorm.context.ConnectionContext
import tanvd.aorm.exceptions.BasicDbException
import tanvd.aorm.insert.InsertWorker
import tanvd.aorm.utils.use
import java.sql.Connection
import javax.sql.DataSource

class Database(val name: String, private val dataSource: DataSource) {
    fun <T> withConnection(body: Connection.() -> T): T {
        return try {
            dataSource.connection.use {
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

fun <T> withDatabase(db: Database, insertWorker: InsertWorker? = null, body: ConnectionContext.() -> T): T = ConnectionContext(db, insertWorker).body()

