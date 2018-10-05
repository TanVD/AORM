package tanvd.aorm

import ru.yandex.clickhouse.ClickHouseUtil
import tanvd.aorm.query.Query

abstract class View(name: String) {
    var name: String = ClickHouseUtil.escape(name)!!

    abstract val query: Query
}

abstract class MaterializedView(name: String) {
    var name: String = ClickHouseUtil.escape(name)!!

    abstract val query: Query
    abstract val engine: Engine
}