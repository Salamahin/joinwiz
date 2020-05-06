package joinwiz.testkit

import joinwiz._
import joinwiz.expression.ExpressionEvaluator

class SeqExpressionEvaluator[L, R] {

  private def compare(first: Any, second: Any): Int = {
    (first, second) match {
      case (f: Int, s: Int)               => Ordering[Int].compare(f, s)
      case (f: Long, s: Long)             => Ordering[Long].compare(f, s)
      case (f: BigInt, s: BigInt)         => Ordering[BigInt].compare(f, s)
      case (f: BigDecimal, s: BigDecimal) => Ordering[BigDecimal].compare(f, s)
      case (f: String, s: String)         => Ordering[String].compare(f, s)
      case _                              => throw new IllegalStateException(s"Comparision of $first, $second is not supported ")
    }
  }

  def apply(l: L, r: R) = new ExpressionEvaluator[L, R, Boolean] {
    override protected def and(left: Boolean, right: Boolean): Boolean = left && right

    override protected def colEqCol(left: LeftTypedColumn[L, _, _], right: RightTypedColumn[R, _, _]): Boolean =
      left(l) == right(r)

    override protected def leftColEqConst(left: LeftTypedColumn[L, _, _], const: Const): Boolean =
      left(l) == const.value

    override protected def rightColEqConst(right: RightTypedColumn[R, _, _], const: Const): Boolean =
      right(r) == const.value

    override protected def colLessCol(left: LeftTypedColumn[L, _, _], right: RightTypedColumn[R, _, _]): Boolean =
      compare(left(l), right(r)) < 0

    override protected def colGreatCol(left: LeftTypedColumn[L, _, _], right: RightTypedColumn[R, _, _]): Boolean =
      compare(left(l), right(r)) > 0

    override protected def colLessOrEqCol(left: LeftTypedColumn[L, _, _], right: RightTypedColumn[R, _, _]): Boolean =
      compare(left(l), right(r)) <= 0

    override protected def colGreatOrEqCol(left: LeftTypedColumn[L, _, _], right: RightTypedColumn[R, _, _]): Boolean =
      compare(left(l), right(r)) >= 0

    override protected def leftColLessConst(left: LeftTypedColumn[L, _, _], const: Const): Boolean =
      compare(left(l), const.value) < 0

    override protected def leftColGreatConst(left: LeftTypedColumn[L, _, _], const: Const): Boolean =
      compare(left(l), const.value) > 0

    override protected def leftColLessOrEqConst(left: LeftTypedColumn[L, _, _], const: Const): Boolean =
      compare(left(l), const.value) <= 0

    override protected def leftCollGreatOrEqConst(left: LeftTypedColumn[L, _, _], const: Const): Boolean =
      compare(left(l), const.value) >= 0

    override protected def rightColLessConst(right: RightTypedColumn[R, _, _], const: Const): Boolean =
      compare(right(r), const.value) < 0

    override protected def rightColGreatConst(right: RightTypedColumn[R, _, _], const: Const): Boolean =
      compare(right(r), const.value) > 0

    override protected def rightColLessOrEqConst(right: RightTypedColumn[R, _, _], const: Const): Boolean =
      compare(right(r), const.value) <= 0

    override protected def rightCollGreatOrEqConst(right: RightTypedColumn[R, _, _], const: Const): Boolean =
      compare(right(r), const.value) >= 0
  }
}
