package joinwiz.expression

import joinwiz._

trait ExpressionEvaluator[T] {

  protected def and(left: T, right: T): T

  protected def equal(left: Value, right: Value): T

  protected def less(left: Value, right: Value): T
  protected def greater(left: Value, right: Value): T
  protected def greaterOrEqual(left: Value, right: Value): T
  protected def lessOrEqual(left: Value, right: Value): T

  final def evaluate(e: Expression): T = e match {
    case And(left, right)         => and(evaluate(left), evaluate(right))
    case Equality(left, right)    => equal(left, right)
    case Less(left, right)        => less(left, right)
    case Greater(left, right)     => greater(left, right)
    case LessOrEq(left, right)    => lessOrEqual(left, right)
    case GreaterOrEq(left, right) => greaterOrEqual(left, right)
  }
}
