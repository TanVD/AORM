package tanvd.aorm

import tanvd.aorm.query.Query

abstract class View(val name: String) {
    abstract val query: Query
}

abstract class MaterializedView(val name: String) {
    abstract val query: Query
    abstract val engine: Engine
}