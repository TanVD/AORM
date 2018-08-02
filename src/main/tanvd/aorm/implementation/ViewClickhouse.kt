package tanvd.aorm.implementation

import tanvd.aorm.Database
import tanvd.aorm.View
import tanvd.aorm.expression.AliasedExpression

object ViewClickhouse {
    fun create(db: Database, view: View) {
        assert(view.query.expressions.all { it is AliasedExpression<*, *, *> })

        db.execute("CREATE VIEW ${view.name} AS ${QueryClickhouse.constructQuery(view.query)}")
    }

    fun exists(db: Database, view: View): Boolean = MetadataClickhouse.existsTable(db, view)

    fun drop(db: Database, view: View) {
        db.execute("DROP TABLE ${view.name};")
    }
}