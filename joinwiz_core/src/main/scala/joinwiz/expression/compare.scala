package joinwiz.expression

import java.sql.{Date, Timestamp}

import joinwiz.Expr.expr
import joinwiz.{Expr, LTCol, RTCol}

final case class LTCol2W[L, R, T] private[joinwiz] (wrapped: LTCol[L, R, T])
final case class RTCol2W[L, R, T] private[joinwiz] (wrapped: RTCol[L, R, T])
final case class LTCol2OptW[L, R, T] private[joinwiz] (wrapped: LTCol[L, R, Option[T]])
final case class RTCol2OptW[L, R, T] private[joinwiz] (wrapped: RTCol[L, R, Option[T]])

trait LowLevelCompareSyntax {
  import org.apache.spark.sql.functions.lit

  implicit def ltColToWrapper[L, R, T](col: LTCol[L, R, T]): LTCol2W[L, R, T]               = LTCol2W[L, R, T](col)
  implicit def rtColToWrapper[L, R, T](col: RTCol[L, R, T]): RTCol2W[L, R, T]               = RTCol2W[L, R, T](col)
  implicit def ltOptColToWrapper[L, R, T](col: LTCol[L, R, Option[T]]): LTCol2OptW[L, R, T] = LTCol2OptW[L, R, T](col)
  implicit def rtOptColToWrapper[L, R, T](col: RTCol[L, R, Option[T]]): RTCol2OptW[L, R, T] = RTCol2OptW[L, R, T](col)

  implicit val dateOrdering: Ordering[Date] = new Ordering[Date] {
    override def compare(x: Date, y: Date): Int = implicitly[Ordering[Long]].compare(x.getTime, y.getTime)
  }

  implicit val timestampOrdering: Ordering[Timestamp] = new Ordering[Timestamp] {
    override def compare(x: Timestamp, y: Timestamp): Int = implicitly[Ordering[Long]].compare(x.getTime, y.getTime)
  }

  implicit class ComparisonResultSyntax(compResult: Option[Int]) {
    def containsEither(expected: Int, orExpected: Int): Boolean = compResult.contains(expected) || compResult.contains(orExpected)
  }

  def compare[T: Ordering](x1: T, x2: T): Option[Int]         = Some(implicitly[Ordering[T]].compare(x1, x2))
  def compare[T: Ordering](x1: T, x2: Option[T]): Option[Int] = x2.map(implicitly[Ordering[T]].compare(x1, _))
  def compare[T: Ordering](x1: Option[T], x2: T): Option[Int] = x1.map(implicitly[Ordering[T]].compare(_, x2))
  def compare[T: Ordering](x1: Option[T], x2: Option[T]): Option[Int] =
    for {
      l <- x1
      r <- x2
    } yield implicitly[Ordering[T]].compare(l, r)

  sealed abstract class LTCol2CompareSyntax[L, R, T: Ordering](thisCol: LTCol[L, R, T]) {
    def <(thatCol: LTCol2W[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column < thatCol.wrapped.column)((l, _) => compare(thisCol(l), thatCol.wrapped(l)).contains(-1))
    def <(thatCol: RTCol2W[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column < thatCol.wrapped.column)((l, r) => compare(thisCol(l), thatCol.wrapped(r)).contains(-1))
    def <(thatCol: LTCol2OptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column < thatCol.wrapped.column)((l, _) => compare(thisCol(l), thatCol.wrapped(l)).contains(-1))
    def <(thatCol: RTCol2OptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column < thatCol.wrapped.column)((l, r) => compare(thisCol(l), thatCol.wrapped(r)).contains(-1))
    def <(const: T): Expr[L, R]                     = expr[L, R](thisCol.column < lit(const))((l, _) => compare(thisCol(l), const).contains(-1))

    def <=(thatCol: LTCol2W[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column <= thatCol.wrapped.column)((l, _) => compare(thisCol(l), thatCol.wrapped(l)).containsEither(-1, 0))
    def <=(thatCol: RTCol2W[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column <= thatCol.wrapped.column)((l, r) => compare(thisCol(l), thatCol.wrapped(r)).containsEither(-1, 0))
    def <=(thatCol: LTCol2OptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column <= thatCol.wrapped.column)((l, _) => compare(thisCol(l), thatCol.wrapped(l)).containsEither(-1, 0))
    def <=(thatCol: RTCol2OptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column <= thatCol.wrapped.column)((l, r) => compare(thisCol(l), thatCol.wrapped(r)).containsEither(-1, 0))
    def <=(const: T): Expr[L, R]                     = expr[L, R](thisCol.column <= lit(const))((l, _) => compare(thisCol(l), const).containsEither(-1, 0))

    def >(thatCol: LTCol2W[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column > thatCol.wrapped.column)((l, _) => compare(thisCol(l), thatCol.wrapped(l)).contains(1))
    def >(thatCol: RTCol2W[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column > thatCol.wrapped.column)((l, r) => compare(thisCol(l), thatCol.wrapped(r)).contains(1))
    def >(thatCol: LTCol2OptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column > thatCol.wrapped.column)((l, _) => compare(thisCol(l), thatCol.wrapped(l)).contains(1))
    def >(thatCol: RTCol2OptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column > thatCol.wrapped.column)((l, r) => compare(thisCol(l), thatCol.wrapped(r)).contains(1))
    def >(const: T): Expr[L, R]                     = expr[L, R](thisCol.column > lit(const))((l, _) => compare(thisCol(l), const).contains(1))

    def >=(thatCol: LTCol2W[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column >= thatCol.wrapped.column)((l, _) => compare(thisCol(l), thatCol.wrapped(l)).containsEither(1, 0))
    def >=(thatCol: RTCol2W[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column >= thatCol.wrapped.column)((l, r) => compare(thisCol(l), thatCol.wrapped(r)).containsEither(1, 0))
    def >=(thatCol: LTCol2OptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column >= thatCol.wrapped.column)((l, _) => compare(thisCol(l), thatCol.wrapped(l)).containsEither(1, 0))
    def >=(thatCol: RTCol2OptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column >= thatCol.wrapped.column)((l, r) => compare(thisCol(l), thatCol.wrapped(r)).containsEither(1, 0))
    def >=(const: T): Expr[L, R]                     = expr[L, R](thisCol.column >= lit(const))((l, _) => compare(thisCol(l), const).containsEither(1, 0))
  }

  sealed abstract class RTCol2CompareSyntax[L, R, T: Ordering](thisCol: RTCol[L, R, T]) {
    def <(thatCol: LTCol2W[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column < thatCol.wrapped.column)((l, r) => compare(thisCol(r), thatCol.wrapped(l)).contains(-1))
    def <(thatCol: RTCol2W[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column < thatCol.wrapped.column)((_, r) => compare(thisCol(r), thatCol.wrapped(r)).contains(-1))
    def <(thatCol: LTCol2OptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column < thatCol.wrapped.column)((l, r) => compare(thisCol(r), thatCol.wrapped(l)).contains(-1))
    def <(thatCol: RTCol2OptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column < thatCol.wrapped.column)((_, r) => compare(thisCol(r), thatCol.wrapped(r)).contains(-1))
    def <(const: T): Expr[L, R]                     = expr[L, R](thisCol.column < lit(const))((_, r) => compare(thisCol(r), const).contains(-1))

    def <=(thatCol: LTCol2W[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column <= thatCol.wrapped.column)((l, r) => compare(thisCol(r), thatCol.wrapped(l)).containsEither(-1, 0))
    def <=(thatCol: RTCol2W[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column <= thatCol.wrapped.column)((_, r) => compare(thisCol(r), thatCol.wrapped(r)).containsEither(-1, 0))
    def <=(thatCol: LTCol2OptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column <= thatCol.wrapped.column)((l, r) => compare(thisCol(r), thatCol.wrapped(l)).containsEither(-1, 0))
    def <=(thatCol: RTCol2OptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column <= thatCol.wrapped.column)((_, r) => compare(thisCol(r), thatCol.wrapped(r)).containsEither(-1, 0))
    def <=(const: T): Expr[L, R]                     = expr[L, R](thisCol.column <= lit(const))((_, r) => compare(thisCol(r), const).containsEither(-1, 0))

    def >(thatCol: LTCol2W[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column > thatCol.wrapped.column)((l, r) => compare(thisCol(r), thatCol.wrapped(l)).contains(1))
    def >(thatCol: RTCol2W[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column > thatCol.wrapped.column)((_, r) => compare(thisCol(r), thatCol.wrapped(r)).contains(1))
    def >(thatCol: LTCol2OptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column > thatCol.wrapped.column)((l, r) => compare(thisCol(r), thatCol.wrapped(l)).contains(1))
    def >(thatCol: RTCol2OptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column > thatCol.wrapped.column)((_, r) => compare(thisCol(r), thatCol.wrapped(r)).contains(1))
    def >(const: T): Expr[L, R]                     = expr[L, R](thisCol.column > lit(const))((_, r) => compare(thisCol(r), const).contains(1))

    def >=(thatCol: LTCol2W[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column >= thatCol.wrapped.column)((l, r) => compare(thisCol(r), thatCol.wrapped(l)).containsEither(1, 0))
    def >=(thatCol: RTCol2W[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column >= thatCol.wrapped.column)((_, r) => compare(thisCol(r), thatCol.wrapped(r)).containsEither(1, 0))
    def >=(thatCol: LTCol2OptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column >= thatCol.wrapped.column)((l, r) => compare(thisCol(r), thatCol.wrapped(l)).containsEither(1, 0))
    def >=(thatCol: RTCol2OptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column >= thatCol.wrapped.column)((_, r) => compare(thisCol(r), thatCol.wrapped(r)).containsEither(1, 0))
    def >=(const: T): Expr[L, R]                     = expr[L, R](thisCol.column >= lit(const))((_, r) => compare(thisCol(r), const).containsEither(1, 0))
  }

  implicit class LTCol2IntCompareSyntax[L, R](thisCol: LTCol[L, R, Int])               extends LTCol2CompareSyntax[L, R, Int](thisCol)
  implicit class LTCol2LongCompareSyntax[L, R](thisCol: LTCol[L, R, Long])             extends LTCol2CompareSyntax[L, R, Long](thisCol)
  implicit class LTCol2ShortCompareSyntax[L, R](thisCol: LTCol[L, R, Short])           extends LTCol2CompareSyntax[L, R, Short](thisCol)
  implicit class LTCol2ByteCompareSyntax[L, R](thisCol: LTCol[L, R, Byte])             extends LTCol2CompareSyntax[L, R, Byte](thisCol)
  implicit class LTCol2FloatCompareSyntax[L, R](thisCol: LTCol[L, R, Float])           extends LTCol2CompareSyntax[L, R, Float](thisCol)
  implicit class LTCol2DoubleCompareSyntax[L, R](thisCol: LTCol[L, R, Double])         extends LTCol2CompareSyntax[L, R, Double](thisCol)
  implicit class LTCol2BigDecimalCompareSyntax[L, R](thisCol: LTCol[L, R, BigDecimal]) extends LTCol2CompareSyntax[L, R, BigDecimal](thisCol)
  implicit class LTCol2DateCompareSyntax[L, R](thisCol: LTCol[L, R, Date])             extends LTCol2CompareSyntax[L, R, Date](thisCol)
  implicit class LTCol2TimestampCompareSyntax[L, R](thisCol: LTCol[L, R, Timestamp])   extends LTCol2CompareSyntax[L, R, Timestamp](thisCol)

  implicit class RTCol2IntCompareSyntax[L, R](thisCol: RTCol[L, R, Int])               extends RTCol2CompareSyntax[L, R, Int](thisCol)
  implicit class RTCol2LongCompareSyntax[L, R](thisCol: RTCol[L, R, Long])             extends RTCol2CompareSyntax[L, R, Long](thisCol)
  implicit class RTCol2ShortCompareSyntax[L, R](thisCol: RTCol[L, R, Short])           extends RTCol2CompareSyntax[L, R, Short](thisCol)
  implicit class RTCol2ByteCompareSyntax[L, R](thisCol: RTCol[L, R, Byte])             extends RTCol2CompareSyntax[L, R, Byte](thisCol)
  implicit class RTCol2FloatCompareSyntax[L, R](thisCol: RTCol[L, R, Float])           extends RTCol2CompareSyntax[L, R, Float](thisCol)
  implicit class RTCol2DoubleCompareSyntax[L, R](thisCol: RTCol[L, R, Double])         extends RTCol2CompareSyntax[L, R, Double](thisCol)
  implicit class RTCol2BigDecimalCompareSyntax[L, R](thisCol: RTCol[L, R, BigDecimal]) extends RTCol2CompareSyntax[L, R, BigDecimal](thisCol)
  implicit class RTCol2DateCompareSyntax[L, R](thisCol: RTCol[L, R, Date])             extends RTCol2CompareSyntax[L, R, Date](thisCol)
  implicit class RTCol2TimestampCompareSyntax[L, R](thisCol: RTCol[L, R, Timestamp])   extends RTCol2CompareSyntax[L, R, Timestamp](thisCol)
}

trait CompareSyntax extends LowLevelCompareSyntax {
  import org.apache.spark.sql.functions.lit

  sealed abstract class OptionalLTCol2CompareSyntax[L, R, T: Ordering](thisCol: LTCol[L, R, Option[T]]) {
    def <(thatCol: LTCol2W[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column < thatCol.wrapped.column)((l, _) => compare(thisCol(l), thatCol.wrapped(l)).contains(-1))
    def <(thatCol: RTCol2W[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column < thatCol.wrapped.column)((l, r) => compare(thisCol(l), thatCol.wrapped(r)).contains(-1))
    def <(thatCol: LTCol2OptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column < thatCol.wrapped.column)((l, _) => compare(thisCol(l), thatCol.wrapped(l)).contains(-1))
    def <(thatCol: RTCol2OptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column < thatCol.wrapped.column)((l, r) => compare(thisCol(l), thatCol.wrapped(r)).contains(-1))
    def <(const: T): Expr[L, R]                     = expr[L, R](thisCol.column < lit(const))((l, _) => compare(thisCol(l), const).contains(-1))

    def <=(thatCol: LTCol2W[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column <= thatCol.wrapped.column)((l, _) => compare(thisCol(l), thatCol.wrapped(l)).containsEither(-1, 0))
    def <=(thatCol: RTCol2W[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column <= thatCol.wrapped.column)((l, r) => compare(thisCol(l), thatCol.wrapped(r)).containsEither(-1, 0))
    def <=(thatCol: LTCol2OptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column <= thatCol.wrapped.column)((l, _) => compare(thisCol(l), thatCol.wrapped(l)).containsEither(-1, 0))
    def <=(thatCol: RTCol2OptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column <= thatCol.wrapped.column)((l, r) => compare(thisCol(l), thatCol.wrapped(r)).containsEither(-1, 0))
    def <=(const: T): Expr[L, R]                     = expr[L, R](thisCol.column <= lit(const))((l, _) => compare(thisCol(l), const).containsEither(-1, 0))

    def >(thatCol: LTCol2W[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column > thatCol.wrapped.column)((l, _) => compare(thisCol(l), thatCol.wrapped(l)).contains(1))
    def >(thatCol: RTCol2W[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column > thatCol.wrapped.column)((l, r) => compare(thisCol(l), thatCol.wrapped(r)).contains(1))
    def >(thatCol: LTCol2OptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column > thatCol.wrapped.column)((l, _) => compare(thisCol(l), thatCol.wrapped(l)).contains(1))
    def >(thatCol: RTCol2OptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column > thatCol.wrapped.column)((l, r) => compare(thisCol(l), thatCol.wrapped(r)).contains(1))
    def >(const: T): Expr[L, R]                     = expr[L, R](thisCol.column > lit(const))((l, _) => compare(thisCol(l), const).contains(1))

    def >=(thatCol: LTCol2W[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column >= thatCol.wrapped.column)((l, _) => compare(thisCol(l), thatCol.wrapped(l)).containsEither(1, 0))
    def >=(thatCol: RTCol2W[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column >= thatCol.wrapped.column)((l, r) => compare(thisCol(l), thatCol.wrapped(r)).containsEither(1, 0))
    def >=(thatCol: LTCol2OptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column >= thatCol.wrapped.column)((l, _) => compare(thisCol(l), thatCol.wrapped(l)).containsEither(1, 0))
    def >=(thatCol: RTCol2OptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column >= thatCol.wrapped.column)((l, r) => compare(thisCol(l), thatCol.wrapped(r)).containsEither(1, 0))
    def >=(const: T): Expr[L, R]                     = expr[L, R](thisCol.column >= lit(const))((l, _) => compare(thisCol(l), const).containsEither(1, 0))
  }

  sealed abstract class OptionalRTCol2CompareSyntax[L, R, T: Ordering](thisCol: RTCol[L, R, Option[T]]) {
    def <(thatCol: LTCol2W[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column < thatCol.wrapped.column)((l, r) => compare(thisCol(r), thatCol.wrapped(l)).contains(-1))
    def <(thatCol: RTCol2W[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column < thatCol.wrapped.column)((_, r) => compare(thisCol(r), thatCol.wrapped(r)).contains(-1))
    def <(thatCol: LTCol2OptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column < thatCol.wrapped.column)((l, r) => compare(thisCol(r), thatCol.wrapped(l)).contains(-1))
    def <(thatCol: RTCol2OptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column < thatCol.wrapped.column)((_, r) => compare(thisCol(r), thatCol.wrapped(r)).contains(-1))
    def <(const: T): Expr[L, R]                     = expr[L, R](thisCol.column < lit(const))((_, r) => compare(thisCol(r), const).contains(-1))

    def <=(thatCol: LTCol2W[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column <= thatCol.wrapped.column)((l, r) => compare(thisCol(r), thatCol.wrapped(l)).containsEither(-1, 0))
    def <=(thatCol: RTCol2W[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column <= thatCol.wrapped.column)((_, r) => compare(thisCol(r), thatCol.wrapped(r)).containsEither(-1, 0))
    def <=(thatCol: LTCol2OptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column <= thatCol.wrapped.column)((l, r) => compare(thisCol(r), thatCol.wrapped(l)).containsEither(-1, 0))
    def <=(thatCol: RTCol2OptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column <= thatCol.wrapped.column)((_, r) => compare(thisCol(r), thatCol.wrapped(r)).containsEither(-1, 0))
    def <=(const: T): Expr[L, R]                     = expr[L, R](thisCol.column <= lit(const))((_, r) => compare(thisCol(r), const).containsEither(-1, 0))

    def >(thatCol: LTCol2W[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column > thatCol.wrapped.column)((l, r) => compare(thisCol(r), thatCol.wrapped(l)).contains(1))
    def >(thatCol: RTCol2W[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column > thatCol.wrapped.column)((_, r) => compare(thisCol(r), thatCol.wrapped(r)).contains(1))
    def >(thatCol: LTCol2OptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column > thatCol.wrapped.column)((l, r) => compare(thisCol(r), thatCol.wrapped(l)).contains(1))
    def >(thatCol: RTCol2OptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column > thatCol.wrapped.column)((_, r) => compare(thisCol(r), thatCol.wrapped(r)).contains(1))
    def >(const: T): Expr[L, R]                     = expr[L, R](thisCol.column > lit(const))((_, r) => compare(thisCol(r), const).contains(1))

    def >=(thatCol: LTCol2W[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column >= thatCol.wrapped.column)((l, r) => compare(thisCol(r), thatCol.wrapped(l)).containsEither(1, 0))
    def >=(thatCol: RTCol2W[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column >= thatCol.wrapped.column)((_, r) => compare(thisCol(r), thatCol.wrapped(r)).containsEither(1, 0))
    def >=(thatCol: LTCol2OptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column >= thatCol.wrapped.column)((l, r) => compare(thisCol(r), thatCol.wrapped(l)).containsEither(1, 0))
    def >=(thatCol: RTCol2OptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column >= thatCol.wrapped.column)((_, r) => compare(thisCol(r), thatCol.wrapped(r)).containsEither(1, 0))
    def >=(const: T): Expr[L, R]                     = expr[L, R](thisCol.column >= lit(const))((_, r) => compare(thisCol(r), const).containsEither(1, 0))
  }

  implicit class OptionalLTCol2IntCompareSyntax[L, R](thisCol: LTCol[L, R, Option[Int]])               extends OptionalLTCol2CompareSyntax[L, R, Int](thisCol)
  implicit class OptionalLTCol2LongCompareSyntax[L, R](thisCol: LTCol[L, R, Option[Long]])             extends OptionalLTCol2CompareSyntax[L, R, Long](thisCol)
  implicit class OptionalLTCol2ShortCompareSyntax[L, R](thisCol: LTCol[L, R, Option[Short]])           extends OptionalLTCol2CompareSyntax[L, R, Short](thisCol)
  implicit class OptionalLTCol2ByteCompareSyntax[L, R](thisCol: LTCol[L, R, Option[Byte]])             extends OptionalLTCol2CompareSyntax[L, R, Byte](thisCol)
  implicit class OptionalLTCol2FloatCompareSyntax[L, R](thisCol: LTCol[L, R, Option[Float]])           extends OptionalLTCol2CompareSyntax[L, R, Float](thisCol)
  implicit class OptionalLTCol2DoubleCompareSyntax[L, R](thisCol: LTCol[L, R, Option[Double]])         extends OptionalLTCol2CompareSyntax[L, R, Double](thisCol)
  implicit class OptionalLTCol2BigDecimalCompareSyntax[L, R](thisCol: LTCol[L, R, Option[BigDecimal]]) extends OptionalLTCol2CompareSyntax[L, R, BigDecimal](thisCol)
  implicit class OptionalLTCol2DateCompareSyntax[L, R](thisCol: LTCol[L, R, Option[Date]])             extends OptionalLTCol2CompareSyntax[L, R, Date](thisCol)
  implicit class OptionalLTCol2TimestampCompareSyntax[L, R](thisCol: LTCol[L, R, Option[Timestamp]])   extends OptionalLTCol2CompareSyntax[L, R, Timestamp](thisCol)

  implicit class OptionalRTCol2IntCompareSyntax[L, R](thisCol: RTCol[L, R, Option[Int]])               extends OptionalRTCol2CompareSyntax[L, R, Int](thisCol)
  implicit class OptionalRTCol2LongCompareSyntax[L, R](thisCol: RTCol[L, R, Option[Long]])             extends OptionalRTCol2CompareSyntax[L, R, Long](thisCol)
  implicit class OptionalRTCol2ShortCompareSyntax[L, R](thisCol: RTCol[L, R, Option[Short]])           extends OptionalRTCol2CompareSyntax[L, R, Short](thisCol)
  implicit class OptionalRTCol2ByteCompareSyntax[L, R](thisCol: RTCol[L, R, Option[Byte]])             extends OptionalRTCol2CompareSyntax[L, R, Byte](thisCol)
  implicit class OptionalRTCol2FloatCompareSyntax[L, R](thisCol: RTCol[L, R, Option[Float]])           extends OptionalRTCol2CompareSyntax[L, R, Float](thisCol)
  implicit class OptionalRTCol2DoubleCompareSyntax[L, R](thisCol: RTCol[L, R, Option[Double]])         extends OptionalRTCol2CompareSyntax[L, R, Double](thisCol)
  implicit class OptionalRTCol2BigDecimalCompareSyntax[L, R](thisCol: RTCol[L, R, Option[BigDecimal]]) extends OptionalRTCol2CompareSyntax[L, R, BigDecimal](thisCol)
  implicit class OptionalRTCol2DateCompareSyntax[L, R](thisCol: RTCol[L, R, Option[Date]])             extends OptionalRTCol2CompareSyntax[L, R, Date](thisCol)
  implicit class OptionalRTCol2TimestampCompareSyntax[L, R](thisCol: RTCol[L, R, Option[Timestamp]])   extends OptionalRTCol2CompareSyntax[L, R, Timestamp](thisCol)
}
