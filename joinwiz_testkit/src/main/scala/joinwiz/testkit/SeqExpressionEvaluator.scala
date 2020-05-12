package joinwiz.testkit

import joinwiz._
import joinwiz.expression.ExpressionEvaluator

import scala.annotation.tailrec

class SeqExpressionEvaluator[L, R] {

  @tailrec
  private def value(from: Any, remained: Seq[String]): Any =
    remained match {
      case Seq()        => from
      case head +: tail => value(from.getClass.getDeclaredMethod(head).invoke(from), tail)
    }

  private def compare(first: Any, second: Any): Int = {
    (first, second) match {
      case (f: Int, s: Int)               => Ordering[Int].compare(f, s)
      case (f: Long, s: Long)             => Ordering[Long].compare(f, s)
      case (f: BigInt, s: BigInt)         => Ordering[BigInt].compare(f, s)
      case (f: BigDecimal, s: BigDecimal) => Ordering[BigDecimal].compare(f, s)
      case (f: String, s: String)         => Ordering[String].compare(f, s)
      case _                              => throw new IllegalStateException(s"Comparision of $first, $second is not supported")
    }
  }

  def apply(l: L, r: R) =
    new ExpressionEvaluator[L, R, Boolean] {
      override protected def and(left: Boolean, right: Boolean): Boolean = left && right

      override protected def colEqCol(left: LeftTypedColumn[_], right: RightTypedColumn[_]): Boolean =
        value(l, left.prefixes) == value(r, right.prefixes)

      override protected def leftColEqConst(left: LeftTypedColumn[_], const: Const): Boolean =
        value(l, left.prefixes) == const.value

      override protected def rightColEqConst(right: RightTypedColumn[_], const: Const): Boolean =
        value(r, right.prefixes) == const.value

      override protected def colLessCol(left: LeftTypedColumn[_], right: RightTypedColumn[_]): Boolean =
        compare(value(l, left.prefixes), value(r, right.prefixes)) < 0

      override protected def colGreatCol(left: LeftTypedColumn[_], right: RightTypedColumn[_]): Boolean =
        compare(value(l, left.prefixes), value(r, right.prefixes)) > 0

      override protected def colLessOrEqCol(left: LeftTypedColumn[_], right: RightTypedColumn[_]): Boolean =
        compare(value(l, left.prefixes), value(r, right.prefixes)) <= 0

      override protected def colGreatOrEqCol(left: LeftTypedColumn[_], right: RightTypedColumn[_]): Boolean =
        compare(value(l, left.prefixes), value(r, right.prefixes)) >= 0

      override protected def leftColLessConst(left: LeftTypedColumn[_], const: Const): Boolean =
        compare(value(l, left.prefixes), const.value) < 0

      override protected def leftColGreatConst(left: LeftTypedColumn[_], const: Const): Boolean =
        compare(value(l, left.prefixes), const.value) > 0

      override protected def leftColLessOrEqConst(left: LeftTypedColumn[_], const: Const): Boolean =
        compare(value(l, left.prefixes), const.value) <= 0

      override protected def leftCollGreatOrEqConst(left: LeftTypedColumn[_], const: Const): Boolean =
        compare(value(l, left.prefixes), const.value) >= 0

      override protected def rightColLessConst(right: RightTypedColumn[_], const: Const): Boolean =
        compare(value(r, right.prefixes), const.value) < 0

      override protected def rightColGreatConst(right: RightTypedColumn[_], const: Const): Boolean =
        compare(value(r, right.prefixes), const.value) > 0

      override protected def rightColLessOrEqConst(right: RightTypedColumn[_], const: Const): Boolean =
        compare(value(r, right.prefixes), const.value) <= 0

      override protected def rightCollGreatOrEqConst(right: RightTypedColumn[_], const: Const): Boolean =
        compare(value(r, right.prefixes), const.value) >= 0
    }
}
