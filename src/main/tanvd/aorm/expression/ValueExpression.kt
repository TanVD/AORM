package tanvd.aorm.expression

import ru.yandex.clickhouse.ClickHouseUtil
import tanvd.aorm.DbType

class ValueExpression<E : Any, out T : DbType<E>>(val name: String, type: T) : Expression<E, T>(type) {
    override fun toSelectListDef(): String = ClickHouseUtil.escape(name)

    override fun toQueryQualifier(): String = ClickHouseUtil.escape(name)
}