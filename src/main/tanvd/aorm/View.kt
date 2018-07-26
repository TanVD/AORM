package tanvd.aorm

import ru.yandex.clickhouse.ClickHouseUtil
import tanvd.aorm.expression.Expression

abstract class MaterializedView(name: String) {
    class MaterializedExpression(val name: String, val expression: Expression<*, DbType<*>>)

    var name: String = ClickHouseUtil.escape(name)

    abstract val expressions: List<Expression<*, DbType<*>>>
}