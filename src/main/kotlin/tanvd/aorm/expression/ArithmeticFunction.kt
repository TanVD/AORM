package tanvd.aorm.expression

import tanvd.aorm.*

typealias NumericExpression = Expression<*, DbNumericPrimitiveType<*>>


abstract class ArithmeticFunction<E:Number, out T: DbNumericPrimitiveType<E>>(type: T) : Expression<E, T>(type)

abstract class ArithmeticOperatorFunction<E:Number, out T: DbNumericPrimitiveType<E>>(type: T, val operator: String, val expr1: NumericExpression, val expr2: NumericExpression) : ArithmeticFunction<E, T>(type) {
    override fun toQueryQualifier(): String = "${expr1.toQueryQualifier()} $operator ${expr2.toQueryQualifier()}"

    override fun toSelectListDef(): String  = toQueryQualifier()
}

abstract class FloatArithmeticOperatorFunction(operator: String, expr1: NumericExpression, expr2: NumericExpression)
    : ArithmeticOperatorFunction<Double, DbFloat64>(DbFloat64(), operator, expr1, expr2)

class Divide(expr1: NumericExpression, expr2: NumericExpression) : FloatArithmeticOperatorFunction(" / ", expr1, expr2)

operator fun NumericExpression.div(expr2: NumericExpression) = Divide(this, expr2)