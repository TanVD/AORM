package tanvd.aorm.implementation

import tanvd.aorm.*
import tanvd.aorm.expression.AliasedExpression

object ViewClickhouse {
    fun create(db: Database, view: View) {
        assert(view.query.expressions.all { it is AliasedExpression<*, *, *> })
        db.execute("CREATE VIEW ${view.name} AS ${QueryClickhouse.constructQuery(view.query)}")
    }

    fun exists(db: Database, view: View): Boolean = MetadataClickhouse.existsView(db, view)

    fun drop(db: Database, view: View) {
        db.execute("DROP TABLE ${view.name}")
    }
}

object MaterializedViewClickhouse {
    fun create(db: Database, view: MaterializedView) {
        assert(view.query.expressions.all { it is AliasedExpression<*, *, *> })
        db.execute("CREATE MATERIALIZED VIEW ${view.name} ENGINE = ${view.engine.toSqlDef()} AS ${QueryClickhouse.constructQuery(view.query)}")
    }

    fun replicatedCreate(db: Database, index: Int, view: MaterializedView) {
        db.execute("CREATE  MATERIALIZED VIEW ${view.name} ENGINE = ${view.engine.toReplicatedSqlDef(index)} AS ${QueryClickhouse.constructQuery(view.query)}")
    }

    fun exists(db: Database, view: MaterializedView): Boolean = MetadataClickhouse.existsMaterializedView(db, view)

    fun drop(db: Database, view: MaterializedView) {
        db.execute("DROP TABLE ${view.name}")
    }
}
