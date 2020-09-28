package joinwiz.expression

import java.sql.{Date, Timestamp}

import joinwiz.Expr.expr
import joinwiz.expression.TColOps.Id
import joinwiz.{Expr, LTCol, RTCol}

trait LowLevelCompareSyntax {
  import org.apache.spark.sql.functions.lit

  implicit val dateOrdering: Ordering[Date] = new Ordering[Date] {
    override def compare(x: Date, y: Date): Int = implicitly[Ordering[Long]].compare(x.getTime, y.getTime)
  }

  implicit val timestampOrdering: Ordering[Timestamp] = new Ordering[Timestamp] {
    override def compare(x: Timestamp, y: Timestamp): Int = implicitly[Ordering[Long]].compare(x.getTime, y.getTime)
  }

  implicit class ComparisonResultSyntax(compResult: Option[Int]) {
    def containsEither(expected: Int, orExpected: Int): Boolean = compResult.contains(expected) || compResult.contains(orExpected)
  }

  sealed abstract class BasicLTColCompareSyntax[F[_], L, R, T: Ordering](thisCol: LTCol[L, R, F[T]])(implicit op: TColOps[F]) {
    def <(thatCol: LTColW[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column < thatCol.wrapped.column)((l, _) => op.compare(thisCol(l), thatCol.wrapped(l))(-1))
    def <(thatCol: RTColW[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column < thatCol.wrapped.column)((l, r) => op.compare(thisCol(l), thatCol.wrapped(r))(-1))
    def <(thatCol: LTColOptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column < thatCol.wrapped.column)((l, _) => op.compare(thisCol(l), thatCol.wrapped(l))(-1))
    def <(thatCol: RTColOptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column < thatCol.wrapped.column)((l, r) => op.compare(thisCol(l), thatCol.wrapped(r))(-1))
    def <(const: T): Expr[L, R]                    = expr[L, R](thisCol.column < lit(const))((l, _) => op.compare(thisCol(l), const)(-1))

    def <=(thatCol: LTColW[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column <= thatCol.wrapped.column)((l, _) => op.compare(thisCol(l), thatCol.wrapped(l))(-1, 0))
    def <=(thatCol: RTColW[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column <= thatCol.wrapped.column)((l, r) => op.compare(thisCol(l), thatCol.wrapped(r))(-1, 0))
    def <=(thatCol: LTColOptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column <= thatCol.wrapped.column)((l, _) => op.compare(thisCol(l), thatCol.wrapped(l))(-1, 0))
    def <=(thatCol: RTColOptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column <= thatCol.wrapped.column)((l, r) => op.compare(thisCol(l), thatCol.wrapped(r))(-1, 0))
    def <=(const: T): Expr[L, R]                    = expr[L, R](thisCol.column <= lit(const))((l, _) => op.compare(thisCol(l), const)(-1, 0))

    def >(thatCol: LTColW[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column > thatCol.wrapped.column)((l, _) => op.compare(thisCol(l), thatCol.wrapped(l))(1))
    def >(thatCol: RTColW[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column > thatCol.wrapped.column)((l, r) => op.compare(thisCol(l), thatCol.wrapped(r))(1))
    def >(thatCol: LTColOptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column > thatCol.wrapped.column)((l, _) => op.compare(thisCol(l), thatCol.wrapped(l))(1))
    def >(thatCol: RTColOptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column > thatCol.wrapped.column)((l, r) => op.compare(thisCol(l), thatCol.wrapped(r))(1))
    def >(const: T): Expr[L, R]                    = expr[L, R](thisCol.column > lit(const))((l, _) => op.compare(thisCol(l), const)(1))

    def >=(thatCol: LTColW[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column >= thatCol.wrapped.column)((l, _) => op.compare(thisCol(l), thatCol.wrapped(l))(1, 0))
    def >=(thatCol: RTColW[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column >= thatCol.wrapped.column)((l, r) => op.compare(thisCol(l), thatCol.wrapped(r))(1, 0))
    def >=(thatCol: LTColOptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column >= thatCol.wrapped.column)((l, _) => op.compare(thisCol(l), thatCol.wrapped(l))(1, 0))
    def >=(thatCol: RTColOptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column >= thatCol.wrapped.column)((l, r) => op.compare(thisCol(l), thatCol.wrapped(r))(1, 0))
    def >=(const: T): Expr[L, R]                    = expr[L, R](thisCol.column >= lit(const))((l, _) => op.compare(thisCol(l), const)(1, 0))
  }

  sealed abstract class BasicRTColCompareSyntax[F[_], L, R, T: Ordering](thisCol: RTCol[L, R, F[T]])(implicit op: TColOps[F]) {
    def <(thatCol: LTColW[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column < thatCol.wrapped.column)((l, r) => op.compare(thisCol(r), thatCol.wrapped(l))(-1))
    def <(thatCol: RTColW[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column < thatCol.wrapped.column)((_, r) => op.compare(thisCol(r), thatCol.wrapped(r))(-1))
    def <(thatCol: LTColOptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column < thatCol.wrapped.column)((l, r) => op.compare(thisCol(r), thatCol.wrapped(l))(-1))
    def <(thatCol: RTColOptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column < thatCol.wrapped.column)((_, r) => op.compare(thisCol(r), thatCol.wrapped(r))(-1))
    def <(const: T): Expr[L, R]                    = expr[L, R](thisCol.column < lit(const))((_, r) => op.compare(thisCol(r), const)(-1))

    def <=(thatCol: LTColW[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column <= thatCol.wrapped.column)((l, r) => op.compare(thisCol(r), thatCol.wrapped(l))(-1, 0))
    def <=(thatCol: RTColW[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column <= thatCol.wrapped.column)((_, r) => op.compare(thisCol(r), thatCol.wrapped(r))(-1, 0))
    def <=(thatCol: LTColOptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column <= thatCol.wrapped.column)((l, r) => op.compare(thisCol(r), thatCol.wrapped(l))(-1, 0))
    def <=(thatCol: RTColOptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column <= thatCol.wrapped.column)((_, r) => op.compare(thisCol(r), thatCol.wrapped(r))(-1, 0))
    def <=(const: T): Expr[L, R]                    = expr[L, R](thisCol.column <= lit(const))((_, r) => op.compare(thisCol(r), const)(-1, 0))

    def >(thatCol: LTColW[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column > thatCol.wrapped.column)((l, r) => op.compare(thisCol(r), thatCol.wrapped(l))(1))
    def >(thatCol: RTColW[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column > thatCol.wrapped.column)((_, r) => op.compare(thisCol(r), thatCol.wrapped(r))(1))
    def >(thatCol: LTColOptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column > thatCol.wrapped.column)((l, r) => op.compare(thisCol(r), thatCol.wrapped(l))(1))
    def >(thatCol: RTColOptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column > thatCol.wrapped.column)((_, r) => op.compare(thisCol(r), thatCol.wrapped(r))(1))
    def >(const: T): Expr[L, R]                    = expr[L, R](thisCol.column > lit(const))((_, r) => op.compare(thisCol(r), const)(1))

    def >=(thatCol: LTColW[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column >= thatCol.wrapped.column)((l, r) => op.compare(thisCol(r), thatCol.wrapped(l))(1, 0))
    def >=(thatCol: RTColW[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column >= thatCol.wrapped.column)((_, r) => op.compare(thisCol(r), thatCol.wrapped(r))(1, 0))
    def >=(thatCol: LTColOptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column >= thatCol.wrapped.column)((l, r) => op.compare(thisCol(r), thatCol.wrapped(l))(1, 0))
    def >=(thatCol: RTColOptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column >= thatCol.wrapped.column)((_, r) => op.compare(thisCol(r), thatCol.wrapped(r))(1, 0))
    def >=(const: T): Expr[L, R]                    = expr[L, R](thisCol.column >= lit(const))((_, r) => op.compare(thisCol(r), const)(1, 0))
  }

  implicit class LTColIntCompareSyntax[L, R](thisCol: LTCol[L, R, Int])               extends BasicLTColCompareSyntax[Id, L, R, Int](thisCol)
  implicit class LTColLongCompareSyntax[L, R](thisCol: LTCol[L, R, Long])             extends BasicLTColCompareSyntax[Id, L, R, Long](thisCol)
  implicit class LTColShortCompareSyntax[L, R](thisCol: LTCol[L, R, Short])           extends BasicLTColCompareSyntax[Id, L, R, Short](thisCol)
  implicit class LTColByteCompareSyntax[L, R](thisCol: LTCol[L, R, Byte])             extends BasicLTColCompareSyntax[Id, L, R, Byte](thisCol)
  implicit class LTColFloatCompareSyntax[L, R](thisCol: LTCol[L, R, Float])           extends BasicLTColCompareSyntax[Id, L, R, Float](thisCol)
  implicit class LTColDoubleCompareSyntax[L, R](thisCol: LTCol[L, R, Double])         extends BasicLTColCompareSyntax[Id, L, R, Double](thisCol)
  implicit class LTColBigDecimalCompareSyntax[L, R](thisCol: LTCol[L, R, BigDecimal]) extends BasicLTColCompareSyntax[Id, L, R, BigDecimal](thisCol)
  implicit class LTColDateCompareSyntax[L, R](thisCol: LTCol[L, R, Date])             extends BasicLTColCompareSyntax[Id, L, R, Date](thisCol)
  implicit class LTColTimestampCompareSyntax[L, R](thisCol: LTCol[L, R, Timestamp])   extends BasicLTColCompareSyntax[Id, L, R, Timestamp](thisCol)

  implicit class RTColIntCompareSyntax[L, R](thisCol: RTCol[L, R, Int])               extends BasicRTColCompareSyntax[Id, L, R, Int](thisCol)
  implicit class RTColLongCompareSyntax[L, R](thisCol: RTCol[L, R, Long])             extends BasicRTColCompareSyntax[Id, L, R, Long](thisCol)
  implicit class RTColShortCompareSyntax[L, R](thisCol: RTCol[L, R, Short])           extends BasicRTColCompareSyntax[Id, L, R, Short](thisCol)
  implicit class RTColByteCompareSyntax[L, R](thisCol: RTCol[L, R, Byte])             extends BasicRTColCompareSyntax[Id, L, R, Byte](thisCol)
  implicit class RTColFloatCompareSyntax[L, R](thisCol: RTCol[L, R, Float])           extends BasicRTColCompareSyntax[Id, L, R, Float](thisCol)
  implicit class RTColDoubleCompareSyntax[L, R](thisCol: RTCol[L, R, Double])         extends BasicRTColCompareSyntax[Id, L, R, Double](thisCol)
  implicit class RTColBigDecimalCompareSyntax[L, R](thisCol: RTCol[L, R, BigDecimal]) extends BasicRTColCompareSyntax[Id, L, R, BigDecimal](thisCol)
  implicit class RTColDateCompareSyntax[L, R](thisCol: RTCol[L, R, Date])             extends BasicRTColCompareSyntax[Id, L, R, Date](thisCol)
  implicit class RTColTimestampCompareSyntax[L, R](thisCol: RTCol[L, R, Timestamp])   extends BasicRTColCompareSyntax[Id, L, R, Timestamp](thisCol)
}

trait CompareSyntax extends LowLevelCompareSyntax {
  implicit class OptionalLTColIntCompareSyntax[L, R](thisCol: LTCol[L, R, Option[Int]])               extends BasicLTColCompareSyntax[Option, L, R, Int](thisCol)
  implicit class OptionalLTColLongCompareSyntax[L, R](thisCol: LTCol[L, R, Option[Long]])             extends BasicLTColCompareSyntax[Option, L, R, Long](thisCol)
  implicit class OptionalLTColShortCompareSyntax[L, R](thisCol: LTCol[L, R, Option[Short]])           extends BasicLTColCompareSyntax[Option, L, R, Short](thisCol)
  implicit class OptionalLTColByteCompareSyntax[L, R](thisCol: LTCol[L, R, Option[Byte]])             extends BasicLTColCompareSyntax[Option, L, R, Byte](thisCol)
  implicit class OptionalLTColFloatCompareSyntax[L, R](thisCol: LTCol[L, R, Option[Float]])           extends BasicLTColCompareSyntax[Option, L, R, Float](thisCol)
  implicit class OptionalLTColDoubleCompareSyntax[L, R](thisCol: LTCol[L, R, Option[Double]])         extends BasicLTColCompareSyntax[Option, L, R, Double](thisCol)
  implicit class OptionalLTColBigDecimalCompareSyntax[L, R](thisCol: LTCol[L, R, Option[BigDecimal]]) extends BasicLTColCompareSyntax[Option, L, R, BigDecimal](thisCol)
  implicit class OptionalLTColDateCompareSyntax[L, R](thisCol: LTCol[L, R, Option[Date]])             extends BasicLTColCompareSyntax[Option, L, R, Date](thisCol)
  implicit class OptionalLTColTimestampCompareSyntax[L, R](thisCol: LTCol[L, R, Option[Timestamp]])   extends BasicLTColCompareSyntax[Option, L, R, Timestamp](thisCol)

  implicit class OptionalRTColIntCompareSyntax[L, R](thisCol: RTCol[L, R, Option[Int]])               extends BasicRTColCompareSyntax[Option, L, R, Int](thisCol)
  implicit class OptionalRTColLongCompareSyntax[L, R](thisCol: RTCol[L, R, Option[Long]])             extends BasicRTColCompareSyntax[Option, L, R, Long](thisCol)
  implicit class OptionalRTColShortCompareSyntax[L, R](thisCol: RTCol[L, R, Option[Short]])           extends BasicRTColCompareSyntax[Option, L, R, Short](thisCol)
  implicit class OptionalRTColByteCompareSyntax[L, R](thisCol: RTCol[L, R, Option[Byte]])             extends BasicRTColCompareSyntax[Option, L, R, Byte](thisCol)
  implicit class OptionalRTColFloatCompareSyntax[L, R](thisCol: RTCol[L, R, Option[Float]])           extends BasicRTColCompareSyntax[Option, L, R, Float](thisCol)
  implicit class OptionalRTColDoubleCompareSyntax[L, R](thisCol: RTCol[L, R, Option[Double]])         extends BasicRTColCompareSyntax[Option, L, R, Double](thisCol)
  implicit class OptionalRTColBigDecimalCompareSyntax[L, R](thisCol: RTCol[L, R, Option[BigDecimal]]) extends BasicRTColCompareSyntax[Option, L, R, BigDecimal](thisCol)
  implicit class OptionalRTColDateCompareSyntax[L, R](thisCol: RTCol[L, R, Option[Date]])             extends BasicRTColCompareSyntax[Option, L, R, Date](thisCol)
  implicit class OptionalRTColTimestampCompareSyntax[L, R](thisCol: RTCol[L, R, Option[Timestamp]])   extends BasicRTColCompareSyntax[Option, L, R, Timestamp](thisCol)
}
