package joinwiz.testkit

import java.sql.{Date, Timestamp}

import joinwiz.expression.ExpressionEvaluator
import joinwiz.{Const, LTCol, RTCol, Value}

class SeqExpressionEvaluator[L, R] {

  private def value[_](left: Any, right: Any, value: Value) = value match {
    case Const(value)      => value
    case lcol: LTCol[_, _] => lcol(left.asInstanceOf[lcol.Orig])
    case rcol: RTCol[_, _] => rcol(right.asInstanceOf[rcol.Orig])
  }

  private def compare(left: Any, right: Any): Option[Int] = (left, right) match {
    case (None, None)                   => None
    case (Some(x), Some(y))             => compare(x, y)
    case (Some(_), None)                => None
    case (None, Some(_))                => None
    case (x: Int, y: Int)               => Some(Ordering[Int].compare(x, y))
    case (x: Long, y: Long)             => Some(Ordering[Long].compare(x, y))
    case (x: Short, y: Short)           => Some(Ordering[Short].compare(x, y))
    case (x: Byte, y: Byte)             => Some(Ordering[Byte].compare(x, y))
    case (x: Float, y: Float)           => Some(Ordering[Float].compare(x, y))
    case (x: Double, y: Double)         => Some(Ordering[Double].compare(x, y))
    case (x: BigDecimal, y: BigDecimal) => Some(Ordering[BigDecimal].compare(x, y))
    case (x: Date, y: Date)             => Some(Ordering[Long].compare(x.getTime, y.getTime))
    case (x: Timestamp, y: Timestamp)   => Some(Ordering[Long].compare(x.getTime, y.getTime))
    case (x, y)                         => throw new IllegalStateException(s"Can't compare $x and $y")
  }

  def apply(l: L, r: R): ExpressionEvaluator[Boolean] = new ExpressionEvaluator[Boolean] {
    override protected def and(left: Boolean, right: Boolean) = left && right

    override protected def equal(left: Value, right: Value) = value(l, r, left) == value(l, r, right)

    override protected def less(left: Value, right: Value) =
      compare(value[L](l, r, left), value[R](l, r, right)).exists(_ < 0)

    override protected def greater(left: Value, right: Value) =
      compare(value[L](l, r, left), value[R](l, r, right)).exists(_ > 0)

    override protected def greaterOrEqual(left: Value, right: Value) =
      compare(value[L](l, r, left), value[R](l, r, right)).exists(_ >= 0)

    override protected def lessOrEqual(left: Value, right: Value) =
      compare(value[L](l, r, left), value[R](l, r, right)).exists(_ <= 0)
  }
}
