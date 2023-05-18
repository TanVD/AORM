package tanvd.aorm

import tanvd.aorm.query.Query
import tanvd.aorm.utils.escapeQuotes

abstract class View(name: String) {
    var name: String = name.escapeQuotes()

    abstract val query: Query
}

abstract class MaterializedView(name: String) {
    var name: String = name.escapeQuotes()

    abstract val query: Query
    abstract val engine: Engine
}