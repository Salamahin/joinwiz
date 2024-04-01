package joinwiz.expression

import joinwiz.expression.JoinCondition.joinCondition
import joinwiz.{LTColumn, RTColumn}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

import java.sql.{Date, Timestamp}
import scala.collection.Set

trait CanCompare[-A, -B] {
  def compare(a: A, b: B): Int
}

object CanCompare {
  private def instance[A, B](func: (A, B) => Int): CanCompare[A, B] = new CanCompare[A, B] {
    override def compare(a: A, b: B): Int = func(a, b)
  }

  implicit val intCompare: CanCompare[Int, Int]                      = instance(_ compare _)
  implicit val shortCompare: CanCompare[Short, Short]                = instance(_ compare _)
  implicit val doubleCompare: CanCompare[Double, Double]             = instance(_ compare _)
  implicit val floatCompare: CanCompare[Float, Float]                = instance(_ compare _)
  implicit val longCompare: CanCompare[Long, Long]                   = instance(_ compare _)
  implicit val byteCompare: CanCompare[Byte, Byte]                   = instance(_ compare _)
  implicit val bigDecimalCompare: CanCompare[BigDecimal, BigDecimal] = instance(_ compare _)
  implicit val dateCompare: CanCompare[Date, Date]                   = instance((a, b) => a.getTime compare b.getTime)
  implicit val timestampCompare: CanCompare[Timestamp, Timestamp]    = instance((a, b) => a.getTime compare b.getTime)

  implicit def canCompareOptionToScalar[A](implicit cc: CanCompare[A, A]): CanCompare[Option[A], A] = instance((a, b) => a.fold(-1)(cc.compare(_, b)))
  implicit def canCompareScalarToOption[A](implicit cc: CanCompare[A, A]): CanCompare[A, Option[A]] = instance((a, b) => b.fold(1)(cc.compare(a, _)))
  implicit def canCompareOptions[A](implicit cc: CanCompare[A, A]): CanCompare[Option[A], Option[A]] = instance {
    case (Some(a), Some(b)) => cc.compare(a, b)
    case (Some(_), None)    => 1
    case (None, Some(_))    => -1
    case (None, None)       => 0
  }
}

trait CanCompareCol[LARG, RARG, L, R] {
  def apply(larg: LARG, rarg: RARG, expectedCodes: Set[Int]): JoinCondition[L, R]
}

trait LowLevelCanCompareDefs {
  def expectedCodeToColumn(codes: Set[Int]): (Column, Column) => Column = {
    if (codes == Set(0)) _ === _
    else if (codes == Set(1)) _ > _
    else if (codes == Set(-1)) _ < _
    else if (codes == Set(0, 1)) _ >= _
    else if (codes == Set(0, -1)) _ <= _
    else throw new IllegalArgumentException(s"Unsupported codes: $codes")
  }

  def instance[LARG, RARG, L, R](func: (LARG, RARG, Set[Int]) => JoinCondition[L, R]): CanCompareCol[LARG, RARG, L, R] = new CanCompareCol[LARG, RARG, L, R] {
    override def apply(larg: LARG, rarg: RARG, expectedCodes: Set[Int]): JoinCondition[L, R] = func(larg, rarg, expectedCodes)
  }

  implicit def ltCompareConst[L, R, T, U](implicit c: CanCompare[T, U]): CanCompareCol[LTColumn[L, R, T], U, L, R] = instance { (larg, rarg, codes) =>
    joinCondition[L, R] { (l, _) => codes contains c.compare(larg.value(l), rarg) }(
      expectedCodeToColumn(codes)(larg.toColumn, lit(rarg))
    )
  }

  implicit def rtCompareConst[L, R, T, U](implicit c: CanCompare[T, U]): CanCompareCol[RTColumn[L, R, T], U, L, R] = instance { (larg, rarg, codes) =>
    joinCondition[L, R] { (_, r) => codes contains c.compare(larg.value(r), rarg) }(
      expectedCodeToColumn(codes)(larg.toColumn, lit(rarg))
    )
  }
}

object CanCompareCol extends LowLevelCanCompareDefs {
  implicit def ltCompareLt[L, R, T, U](implicit c: CanCompare[T, U]): CanCompareCol[LTColumn[L, R, T], LTColumn[L, R, U], L, R] = instance { (larg, rarg, codes) =>
    joinCondition[L, R] { (l, _) => codes contains c.compare(larg.value(l), rarg.value(l)) }(
      expectedCodeToColumn(codes)(larg.toColumn, rarg.toColumn)
    )
  }

  implicit def ltCompareRt[L, R, T, U](implicit c: CanCompare[T, U]): CanCompareCol[LTColumn[L, R, T], RTColumn[L, R, U], L, R] = instance { (larg, rarg, codes) =>
    joinCondition[L, R] { (l, r) => codes contains c.compare(larg.value(l), rarg.value(r)) }(
      expectedCodeToColumn(codes)(larg.toColumn, rarg.toColumn)
    )
  }

  implicit def rtCompareRt[L, R, T, U](implicit c: CanCompare[T, U]): CanCompareCol[RTColumn[L, R, T], RTColumn[L, R, U], L, R] = instance { (larg, rarg, codes) =>
    joinCondition[L, R] { (_, r) => codes contains c.compare(larg.value(r), rarg.value(r)) }(
      expectedCodeToColumn(codes)(larg.toColumn, rarg.toColumn)
    )
  }

  implicit def rtCompareLt[L, R, T, U](implicit c: CanCompare[T, U]): CanCompareCol[RTColumn[L, R, T], LTColumn[L, R, U], L, R] = instance { (larg, rarg, codes) =>
    joinCondition[L, R] { (l, r) => codes contains c.compare(larg.value(r), rarg.value(l)) }(
      expectedCodeToColumn(codes)(larg.toColumn, rarg.toColumn)
    )
  }
}

trait CompareSyntax {
  implicit class CanCompareSyntax[K](k: K) {
    def >[S, L, R](s: S)(implicit cc: CanCompareCol[K, S, L, R]): JoinCondition[L, R]  = cc(k, s, Set(1))
    def <[S, L, R](s: S)(implicit cc: CanCompareCol[K, S, L, R]): JoinCondition[L, R]  = cc(k, s, Set(-1))
    def >=[S, L, R](s: S)(implicit cc: CanCompareCol[K, S, L, R]): JoinCondition[L, R] = cc(k, s, Set(0, 1))
    def <=[S, L, R](s: S)(implicit cc: CanCompareCol[K, S, L, R]): JoinCondition[L, R] = cc(k, s, Set(0, -1))
  }
}
