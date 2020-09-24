package joinwiz.expression

import java.sql.{Date, Timestamp}

import joinwiz.Expr.expr
import joinwiz.{Expr, LTCol2, RTCol2}

final case class LTCol2W[O, T] private[joinwiz] (wrapped: LTCol2[O, T])
final case class RTCol2W[O, T] private[joinwiz] (wrapped: RTCol2[O, T])
final case class LTCol2OptW[O, T] private[joinwiz] (wrapped: LTCol2[O, Option[T]])
final case class RTCol2OptW[O, T] private[joinwiz] (wrapped: RTCol2[O, Option[T]])

trait LowLevelCompareSyntax {
  import org.apache.spark.sql.functions.lit

  implicit def ltColToWrapper[O, T](col: LTCol2[O, T]): LTCol2W[O, T]               = LTCol2W[O, T](col)
  implicit def rtColToWrapper[O, T](col: RTCol2[O, T]): RTCol2W[O, T]               = RTCol2W[O, T](col)
  implicit def ltOptColToWrapper[O, T](col: LTCol2[O, Option[T]]): LTCol2OptW[O, T] = LTCol2OptW[O, T](col)
  implicit def rtOptColToWrapper[O, T](col: RTCol2[O, Option[T]]): RTCol2OptW[O, T] = RTCol2OptW[O, T](col)

  implicit val dateOrdering: Ordering[Date] = new Ordering[Date] {
    override def compare(x: Date, y: Date): Int = implicitly[Ordering[Long]].compare(x.getTime, y.getTime)
  }

  implicit val timestampOrdering: Ordering[Timestamp] = new Ordering[Timestamp] {
    override def compare(x: Timestamp, y: Timestamp): Int = implicitly[Ordering[Long]].compare(x.getTime, y.getTime)
  }

  implicit class ComparisonResultSyntax(compResult: Option[Int]) {
    def containsEither(expected: Int, orExpected: Int): Boolean = compResult.contains(expected) || compResult.contains(orExpected)
  }

  def compare[T: Ordering](x1: T, x2: T): Option[Int] = {
    val a = Some(implicitly[Ordering[T]].compare(x1, x2))
    a
  }
  def compare[T: Ordering](x1: T, x2: Option[T]): Option[Int] = x2.map(implicitly[Ordering[T]].compare(x1, _))
  def compare[T: Ordering](x1: Option[T], x2: T): Option[Int] = {
    val a = x1.map(implicitly[Ordering[T]].compare(_, x2))
    a
  }
  def compare[T: Ordering](x1: Option[T], x2: Option[T]): Option[Int] =
    for {
      l <- x1
      r <- x2
    } yield implicitly[Ordering[T]].compare(l, r)

  sealed abstract class LTCol2CompareSyntax[L, T: Ordering](thisCol: LTCol2[L, T]) {
    def <[R](thatCol: LTCol2W[L, T]): Expr[L, R]    = expr[L, R](thisCol.column < thatCol.wrapped.column)((l, _) => compare(thisCol(l), thatCol.wrapped(l)).contains(-1))
    def <[R](thatCol: RTCol2W[R, T]): Expr[L, R]    = expr[L, R](thisCol.column < thatCol.wrapped.column)((l, r) => compare(thisCol(l), thatCol.wrapped(r)).contains(-1))
    def <[R](thatCol: LTCol2OptW[L, T]): Expr[L, R] = expr[L, R](thisCol.column < thatCol.wrapped.column)((l, _) => compare(thisCol(l), thatCol.wrapped(l)).contains(-1))
    def <[R](thatCol: RTCol2OptW[R, T]): Expr[L, R] = expr[L, R](thisCol.column < thatCol.wrapped.column)((l, r) => compare(thisCol(l), thatCol.wrapped(r)).contains(-1))
    def <[R](const: T): Expr[L, R]                  = expr[L, R](thisCol.column < lit(const))((l, _) => compare(thisCol(l), const).contains(-1))

    def <=[R](thatCol: LTCol2W[L, T]): Expr[L, R]    = expr[L, R](thisCol.column <= thatCol.wrapped.column)((l, _) => compare(thisCol(l), thatCol.wrapped(l)).containsEither(-1, 0))
    def <=[R](thatCol: RTCol2W[R, T]): Expr[L, R]    = expr[L, R](thisCol.column <= thatCol.wrapped.column)((l, r) => compare(thisCol(l), thatCol.wrapped(r)).containsEither(-1, 0))
    def <=[R](thatCol: LTCol2OptW[L, T]): Expr[L, R] = expr[L, R](thisCol.column <= thatCol.wrapped.column)((l, _) => compare(thisCol(l), thatCol.wrapped(l)).containsEither(-1, 0))
    def <=[R](thatCol: RTCol2OptW[R, T]): Expr[L, R] = expr[L, R](thisCol.column <= thatCol.wrapped.column)((l, r) => compare(thisCol(l), thatCol.wrapped(r)).containsEither(-1, 0))
    def <=[R](const: T): Expr[L, R]                  = expr[L, R](thisCol.column <= lit(const))((l, _) => compare(thisCol(l), const).containsEither(-1, 0))

    def >[R](thatCol: LTCol2W[L, T]): Expr[L, R]    = expr[L, R](thisCol.column > thatCol.wrapped.column)((l, _) => compare(thisCol(l), thatCol.wrapped(l)).contains(1))
    def >[R](thatCol: RTCol2W[R, T]): Expr[L, R]    = expr[L, R](thisCol.column > thatCol.wrapped.column)((l, r) => compare(thisCol(l), thatCol.wrapped(r)).contains(1))
    def >[R](thatCol: LTCol2OptW[L, T]): Expr[L, R] = expr[L, R](thisCol.column > thatCol.wrapped.column)((l, _) => compare(thisCol(l), thatCol.wrapped(l)).contains(1))
    def >[R](thatCol: RTCol2OptW[R, T]): Expr[L, R] = expr[L, R](thisCol.column > thatCol.wrapped.column)((l, r) => compare(thisCol(l), thatCol.wrapped(r)).contains(1))
    def >[R](const: T): Expr[L, R]                  = expr[L, R](thisCol.column > lit(const))((l, _) => compare(thisCol(l), const).contains(1))

    def >=[R](thatCol: LTCol2W[L, T]): Expr[L, R]    = expr[L, R](thisCol.column >= thatCol.wrapped.column)((l, _) => compare(thisCol(l), thatCol.wrapped(l)).containsEither(1, 0))
    def >=[R](thatCol: RTCol2W[R, T]): Expr[L, R]    = expr[L, R](thisCol.column >= thatCol.wrapped.column)((l, r) => compare(thisCol(l), thatCol.wrapped(r)).containsEither(1, 0))
    def >=[R](thatCol: LTCol2OptW[L, T]): Expr[L, R] = expr[L, R](thisCol.column >= thatCol.wrapped.column)((l, _) => compare(thisCol(l), thatCol.wrapped(l)).containsEither(1, 0))
    def >=[R](thatCol: RTCol2OptW[R, T]): Expr[L, R] = expr[L, R](thisCol.column >= thatCol.wrapped.column)((l, r) => compare(thisCol(l), thatCol.wrapped(r)).containsEither(1, 0))
    def >=[R](const: T): Expr[L, R]                  = expr[L, R](thisCol.column >= lit(const))((l, _) => compare(thisCol(l), const).containsEither(1, 0))
  }

  sealed abstract class RTCol2CompareSyntax[R, T: Ordering](thisCol: RTCol2[R, T]) {
    def <[L](thatCol: LTCol2W[L, T]): Expr[L, R]    = expr[L, R](thisCol.column < thatCol.wrapped.column)((l, r) => compare(thisCol(r), thatCol.wrapped(l)).contains(-1))
    def <[L](thatCol: RTCol2W[R, T]): Expr[L, R]    = expr[L, R](thisCol.column < thatCol.wrapped.column)((_, r) => compare(thisCol(r), thatCol.wrapped(r)).contains(-1))
    def <[L](thatCol: LTCol2OptW[L, T]): Expr[L, R] = expr[L, R](thisCol.column < thatCol.wrapped.column)((l, r) => compare(thisCol(r), thatCol.wrapped(l)).contains(-1))
    def <[L](thatCol: RTCol2OptW[R, T]): Expr[L, R] = expr[L, R](thisCol.column < thatCol.wrapped.column)((_, r) => compare(thisCol(r), thatCol.wrapped(r)).contains(-1))
    def <[L](const: T): Expr[L, R]                  = expr[L, R](thisCol.column < lit(const))((_, r) => compare(thisCol(r), const).contains(-1))

    def <=[L](thatCol: LTCol2W[L, T]): Expr[L, R]    = expr[L, R](thisCol.column <= thatCol.wrapped.column)((l, r) => compare(thisCol(r), thatCol.wrapped(l)).containsEither(-1, 0))
    def <=[L](thatCol: RTCol2W[R, T]): Expr[L, R]    = expr[L, R](thisCol.column <= thatCol.wrapped.column)((_, r) => compare(thisCol(r), thatCol.wrapped(r)).containsEither(-1, 0))
    def <=[L](thatCol: LTCol2OptW[L, T]): Expr[L, R] = expr[L, R](thisCol.column <= thatCol.wrapped.column)((l, r) => compare(thisCol(r), thatCol.wrapped(l)).containsEither(-1, 0))
    def <=[L](thatCol: RTCol2OptW[R, T]): Expr[L, R] = expr[L, R](thisCol.column <= thatCol.wrapped.column)((_, r) => compare(thisCol(r), thatCol.wrapped(r)).containsEither(-1, 0))
    def <=[L](const: T): Expr[L, R]                  = expr[L, R](thisCol.column <= lit(const))((_, r) => compare(thisCol(r), const).containsEither(-1, 0))

    def >[L](thatCol: LTCol2W[L, T]): Expr[L, R]    = expr[L, R](thisCol.column > thatCol.wrapped.column)((l, r) => compare(thisCol(r), thatCol.wrapped(l)).contains(1))
    def >[L](thatCol: RTCol2W[R, T]): Expr[L, R]    = expr[L, R](thisCol.column > thatCol.wrapped.column)((_, r) => compare(thisCol(r), thatCol.wrapped(r)).contains(1))
    def >[L](thatCol: LTCol2OptW[L, T]): Expr[L, R] = expr[L, R](thisCol.column > thatCol.wrapped.column)((l, r) => compare(thisCol(r), thatCol.wrapped(l)).contains(1))
    def >[L](thatCol: RTCol2OptW[R, T]): Expr[L, R] = expr[L, R](thisCol.column > thatCol.wrapped.column)((_, r) => compare(thisCol(r), thatCol.wrapped(r)).contains(1))
    def >[L](const: T): Expr[L, R]                  = expr[L, R](thisCol.column > lit(const))((_, r) => compare(thisCol(r), const).contains(1))

    def >=[L](thatCol: LTCol2W[L, T]): Expr[L, R]    = expr[L, R](thisCol.column >= thatCol.wrapped.column)((l, r) => compare(thisCol(r), thatCol.wrapped(l)).containsEither(1, 0))
    def >=[L](thatCol: RTCol2W[R, T]): Expr[L, R]    = expr[L, R](thisCol.column >= thatCol.wrapped.column)((_, r) => compare(thisCol(r), thatCol.wrapped(r)).containsEither(1, 0))
    def >=[L](thatCol: LTCol2OptW[L, T]): Expr[L, R] = expr[L, R](thisCol.column >= thatCol.wrapped.column)((l, r) => compare(thisCol(r), thatCol.wrapped(l)).containsEither(1, 0))
    def >=[L](thatCol: RTCol2OptW[R, T]): Expr[L, R] = expr[L, R](thisCol.column >= thatCol.wrapped.column)((_, r) => compare(thisCol(r), thatCol.wrapped(r)).containsEither(1, 0))
    def >=[L](const: T): Expr[L, R]                  = expr[L, R](thisCol.column >= lit(const))((_, r) => compare(thisCol(r), const).containsEither(1, 0))
  }

  implicit class LTCol2IntCompareSyntax[L](thisCol: LTCol2[L, Int])               extends LTCol2CompareSyntax[L, Int](thisCol)
  implicit class LTCol2LongCompareSyntax[L](thisCol: LTCol2[L, Long])             extends LTCol2CompareSyntax[L, Long](thisCol)
  implicit class LTCol2ShortCompareSyntax[L](thisCol: LTCol2[L, Short])           extends LTCol2CompareSyntax[L, Short](thisCol)
  implicit class LTCol2ByteCompareSyntax[L](thisCol: LTCol2[L, Byte])             extends LTCol2CompareSyntax[L, Byte](thisCol)
  implicit class LTCol2FloatCompareSyntax[L](thisCol: LTCol2[L, Float])           extends LTCol2CompareSyntax[L, Float](thisCol)
  implicit class LTCol2DoubleCompareSyntax[L](thisCol: LTCol2[L, Double])         extends LTCol2CompareSyntax[L, Double](thisCol)
  implicit class LTCol2BigDecimalCompareSyntax[L](thisCol: LTCol2[L, BigDecimal]) extends LTCol2CompareSyntax[L, BigDecimal](thisCol)
  implicit class LTCol2DateCompareSyntax[L](thisCol: LTCol2[L, Date])             extends LTCol2CompareSyntax[L, Date](thisCol)
  implicit class LTCol2TimestampCompareSyntax[L](thisCol: LTCol2[L, Timestamp])   extends LTCol2CompareSyntax[L, Timestamp](thisCol)

  implicit class RTCol2IntCompareSyntax[R](thisCol: RTCol2[R, Int])               extends RTCol2CompareSyntax[R, Int](thisCol)
  implicit class RTCol2LongCompareSyntax[R](thisCol: RTCol2[R, Long])             extends RTCol2CompareSyntax[R, Long](thisCol)
  implicit class RTCol2ShortCompareSyntax[R](thisCol: RTCol2[R, Short])           extends RTCol2CompareSyntax[R, Short](thisCol)
  implicit class RTCol2ByteCompareSyntax[R](thisCol: RTCol2[R, Byte])             extends RTCol2CompareSyntax[R, Byte](thisCol)
  implicit class RTCol2FloatCompareSyntax[R](thisCol: RTCol2[R, Float])           extends RTCol2CompareSyntax[R, Float](thisCol)
  implicit class RTCol2DoubleCompareSyntax[R](thisCol: RTCol2[R, Double])         extends RTCol2CompareSyntax[R, Double](thisCol)
  implicit class RTCol2BigDecimalCompareSyntax[R](thisCol: RTCol2[R, BigDecimal]) extends RTCol2CompareSyntax[R, BigDecimal](thisCol)
  implicit class RTCol2DateCompareSyntax[R](thisCol: RTCol2[R, Date])             extends RTCol2CompareSyntax[R, Date](thisCol)
  implicit class RTCol2TimestampCompareSyntax[R](thisCol: RTCol2[R, Timestamp])   extends RTCol2CompareSyntax[R, Timestamp](thisCol)
}

trait CompareSyntax extends LowLevelCompareSyntax {
  import org.apache.spark.sql.functions.lit

  sealed abstract class OptionalLTCol2CompareSyntax[L, T: Ordering](thisCol: LTCol2[L, Option[T]]) {
    def <[R](thatCol: LTCol2W[L, T]): Expr[L, R]    = expr[L, R](thisCol.column < thatCol.wrapped.column)((l, _) => compare(thisCol(l), thatCol.wrapped(l)).contains(-1))
    def <[R](thatCol: RTCol2W[R, T]): Expr[L, R]    = expr[L, R](thisCol.column < thatCol.wrapped.column)((l, r) => compare(thisCol(l), thatCol.wrapped(r)).contains(-1))
    def <[R](thatCol: LTCol2OptW[L, T]): Expr[L, R] = expr[L, R](thisCol.column < thatCol.wrapped.column)((l, _) => compare(thisCol(l), thatCol.wrapped(l)).contains(-1))
    def <[R](thatCol: RTCol2OptW[R, T]): Expr[L, R] = expr[L, R](thisCol.column < thatCol.wrapped.column)((l, r) => compare(thisCol(l), thatCol.wrapped(r)).contains(-1))
    def <[R](const: T): Expr[L, R]                  = expr[L, R](thisCol.column < lit(const))((l, _) => compare(thisCol(l), const).contains(-1))

    def <=[R](thatCol: LTCol2W[L, T]): Expr[L, R]    = expr[L, R](thisCol.column <= thatCol.wrapped.column)((l, _) => compare(thisCol(l), thatCol.wrapped(l)).containsEither(-1, 0))
    def <=[R](thatCol: RTCol2W[R, T]): Expr[L, R]    = expr[L, R](thisCol.column <= thatCol.wrapped.column)((l, r) => compare(thisCol(l), thatCol.wrapped(r)).containsEither(-1, 0))
    def <=[R](thatCol: LTCol2OptW[L, T]): Expr[L, R] = expr[L, R](thisCol.column <= thatCol.wrapped.column)((l, _) => compare(thisCol(l), thatCol.wrapped(l)).containsEither(-1, 0))
    def <=[R](thatCol: RTCol2OptW[R, T]): Expr[L, R] = expr[L, R](thisCol.column <= thatCol.wrapped.column)((l, r) => compare(thisCol(l), thatCol.wrapped(r)).containsEither(-1, 0))
    def <=[R](const: T): Expr[L, R]                  = expr[L, R](thisCol.column <= lit(const))((l, _) => compare(thisCol(l), const).containsEither(-1, 0))

    def >[R](thatCol: LTCol2W[L, T]): Expr[L, R]    = expr[L, R](thisCol.column > thatCol.wrapped.column)((l, _) => compare(thisCol(l), thatCol.wrapped(l)).contains(1))
    def >[R](thatCol: RTCol2W[R, T]): Expr[L, R]    = expr[L, R](thisCol.column > thatCol.wrapped.column)((l, r) => compare(thisCol(l), thatCol.wrapped(r)).contains(1))
    def >[R](thatCol: LTCol2OptW[L, T]): Expr[L, R] = expr[L, R](thisCol.column > thatCol.wrapped.column)((l, _) => compare(thisCol(l), thatCol.wrapped(l)).contains(1))
    def >[R](thatCol: RTCol2OptW[R, T]): Expr[L, R] = expr[L, R](thisCol.column > thatCol.wrapped.column)((l, r) => compare(thisCol(l), thatCol.wrapped(r)).contains(1))
    def >[R](const: T): Expr[L, R]                  = expr[L, R](thisCol.column > lit(const))((l, _) => compare(thisCol(l), const).contains(1))

    def >=[R](thatCol: LTCol2W[L, T]): Expr[L, R]    = expr[L, R](thisCol.column >= thatCol.wrapped.column)((l, _) => compare(thisCol(l), thatCol.wrapped(l)).containsEither(1, 0))
    def >=[R](thatCol: RTCol2W[R, T]): Expr[L, R]    = expr[L, R](thisCol.column >= thatCol.wrapped.column)((l, r) => compare(thisCol(l), thatCol.wrapped(r)).containsEither(1, 0))
    def >=[R](thatCol: LTCol2OptW[L, T]): Expr[L, R] = expr[L, R](thisCol.column >= thatCol.wrapped.column)((l, _) => compare(thisCol(l), thatCol.wrapped(l)).containsEither(1, 0))
    def >=[R](thatCol: RTCol2OptW[R, T]): Expr[L, R] = expr[L, R](thisCol.column >= thatCol.wrapped.column)((l, r) => compare(thisCol(l), thatCol.wrapped(r)).containsEither(1, 0))
    def >=[R](const: T): Expr[L, R]                  = expr[L, R](thisCol.column >= lit(const))((l, _) => compare(thisCol(l), const).containsEither(1, 0))
  }

  sealed abstract class OptionalRTCol2CompareSyntax[R, T: Ordering](thisCol: RTCol2[R, Option[T]]) {
    def <[L](thatCol: LTCol2W[L, T]): Expr[L, R]    = expr[L, R](thisCol.column < thatCol.wrapped.column)((l, r) => compare(thisCol(r), thatCol.wrapped(l)).contains(-1))
    def <[L](thatCol: RTCol2W[R, T]): Expr[L, R]    = expr[L, R](thisCol.column < thatCol.wrapped.column)((_, r) => compare(thisCol(r), thatCol.wrapped(r)).contains(-1))
    def <[L](thatCol: LTCol2OptW[L, T]): Expr[L, R] = expr[L, R](thisCol.column < thatCol.wrapped.column)((l, r) => compare(thisCol(r), thatCol.wrapped(l)).contains(-1))
    def <[L](thatCol: RTCol2OptW[R, T]): Expr[L, R] = expr[L, R](thisCol.column < thatCol.wrapped.column)((_, r) => compare(thisCol(r), thatCol.wrapped(r)).contains(-1))
    def <[L](const: T): Expr[L, R]                  = expr[L, R](thisCol.column < lit(const))((_, r) => compare(thisCol(r), const).contains(-1))

    def <=[L](thatCol: LTCol2W[L, T]): Expr[L, R]    = expr[L, R](thisCol.column <= thatCol.wrapped.column)((l, r) => compare(thisCol(r), thatCol.wrapped(l)).containsEither(-1, 0))
    def <=[L](thatCol: RTCol2W[R, T]): Expr[L, R]    = expr[L, R](thisCol.column <= thatCol.wrapped.column)((_, r) => compare(thisCol(r), thatCol.wrapped(r)).containsEither(-1, 0))
    def <=[L](thatCol: LTCol2OptW[L, T]): Expr[L, R] = expr[L, R](thisCol.column <= thatCol.wrapped.column)((l, r) => compare(thisCol(r), thatCol.wrapped(l)).containsEither(-1, 0))
    def <=[L](thatCol: RTCol2OptW[R, T]): Expr[L, R] = expr[L, R](thisCol.column <= thatCol.wrapped.column)((_, r) => compare(thisCol(r), thatCol.wrapped(r)).containsEither(-1, 0))
    def <=[L](const: T): Expr[L, R]                  = expr[L, R](thisCol.column <= lit(const))((_, r) => compare(thisCol(r), const).containsEither(-1, 0))

    def >[L](thatCol: LTCol2W[L, T]): Expr[L, R]    = expr[L, R](thisCol.column > thatCol.wrapped.column)((l, r) => compare(thisCol(r), thatCol.wrapped(l)).contains(1))
    def >[L](thatCol: RTCol2W[R, T]): Expr[L, R]    = expr[L, R](thisCol.column > thatCol.wrapped.column)((_, r) => compare(thisCol(r), thatCol.wrapped(r)).contains(1))
    def >[L](thatCol: LTCol2OptW[L, T]): Expr[L, R] = expr[L, R](thisCol.column > thatCol.wrapped.column)((l, r) => compare(thisCol(r), thatCol.wrapped(l)).contains(1))
    def >[L](thatCol: RTCol2OptW[R, T]): Expr[L, R] = expr[L, R](thisCol.column > thatCol.wrapped.column)((_, r) => compare(thisCol(r), thatCol.wrapped(r)).contains(1))
    def >[L](const: T): Expr[L, R]                  = expr[L, R](thisCol.column > lit(const))((_, r) => compare(thisCol(r), const).contains(1))

    def >=[L](thatCol: LTCol2W[L, T]): Expr[L, R]    = expr[L, R](thisCol.column >= thatCol.wrapped.column)((l, r) => compare(thisCol(r), thatCol.wrapped(l)).containsEither(1, 0))
    def >=[L](thatCol: RTCol2W[R, T]): Expr[L, R]    = expr[L, R](thisCol.column >= thatCol.wrapped.column)((_, r) => compare(thisCol(r), thatCol.wrapped(r)).containsEither(1, 0))
    def >=[L](thatCol: LTCol2OptW[L, T]): Expr[L, R] = expr[L, R](thisCol.column >= thatCol.wrapped.column)((l, r) => compare(thisCol(r), thatCol.wrapped(l)).containsEither(1, 0))
    def >=[L](thatCol: RTCol2OptW[R, T]): Expr[L, R] = expr[L, R](thisCol.column >= thatCol.wrapped.column)((_, r) => compare(thisCol(r), thatCol.wrapped(r)).containsEither(1, 0))
    def >=[L](const: T): Expr[L, R]                  = expr[L, R](thisCol.column >= lit(const))((_, r) => compare(thisCol(r), const).containsEither(1, 0))
  }

  implicit class OptionalLTCol2IntCompareSyntax[L](thisCol: LTCol2[L, Option[Int]])               extends OptionalLTCol2CompareSyntax[L, Int](thisCol)
  implicit class OptionalLTCol2LongCompareSyntax[L](thisCol: LTCol2[L, Option[Long]])             extends OptionalLTCol2CompareSyntax[L, Long](thisCol)
  implicit class OptionalLTCol2ShortCompareSyntax[L](thisCol: LTCol2[L, Option[Short]])           extends OptionalLTCol2CompareSyntax[L, Short](thisCol)
  implicit class OptionalLTCol2ByteCompareSyntax[L](thisCol: LTCol2[L, Option[Byte]])             extends OptionalLTCol2CompareSyntax[L, Byte](thisCol)
  implicit class OptionalLTCol2FloatCompareSyntax[L](thisCol: LTCol2[L, Option[Float]])           extends OptionalLTCol2CompareSyntax[L, Float](thisCol)
  implicit class OptionalLTCol2DoubleCompareSyntax[L](thisCol: LTCol2[L, Option[Double]])         extends OptionalLTCol2CompareSyntax[L, Double](thisCol)
  implicit class OptionalLTCol2BigDecimalCompareSyntax[L](thisCol: LTCol2[L, Option[BigDecimal]]) extends OptionalLTCol2CompareSyntax[L, BigDecimal](thisCol)
  implicit class OptionalLTCol2DateCompareSyntax[L](thisCol: LTCol2[L, Option[Date]])             extends OptionalLTCol2CompareSyntax[L, Date](thisCol)
  implicit class OptionalLTCol2TimestampCompareSyntax[L](thisCol: LTCol2[L, Option[Timestamp]])   extends OptionalLTCol2CompareSyntax[L, Timestamp](thisCol)

  implicit class OptionalRTCol2IntCompareSyntax[R](thisCol: RTCol2[R, Option[Int]])               extends OptionalRTCol2CompareSyntax[R, Int](thisCol)
  implicit class OptionalRTCol2LongCompareSyntax[R](thisCol: RTCol2[R, Option[Long]])             extends OptionalRTCol2CompareSyntax[R, Long](thisCol)
  implicit class OptionalRTCol2ShortCompareSyntax[R](thisCol: RTCol2[R, Option[Short]])           extends OptionalRTCol2CompareSyntax[R, Short](thisCol)
  implicit class OptionalRTCol2ByteCompareSyntax[R](thisCol: RTCol2[R, Option[Byte]])             extends OptionalRTCol2CompareSyntax[R, Byte](thisCol)
  implicit class OptionalRTCol2FloatCompareSyntax[R](thisCol: RTCol2[R, Option[Float]])           extends OptionalRTCol2CompareSyntax[R, Float](thisCol)
  implicit class OptionalRTCol2DoubleCompareSyntax[R](thisCol: RTCol2[R, Option[Double]])         extends OptionalRTCol2CompareSyntax[R, Double](thisCol)
  implicit class OptionalRTCol2BigDecimalCompareSyntax[R](thisCol: RTCol2[R, Option[BigDecimal]]) extends OptionalRTCol2CompareSyntax[R, BigDecimal](thisCol)
  implicit class OptionalRTCol2DateCompareSyntax[R](thisCol: RTCol2[R, Option[Date]])             extends OptionalRTCol2CompareSyntax[R, Date](thisCol)
  implicit class OptionalRTCol2TimestampCompareSyntax[R](thisCol: RTCol2[R, Option[Timestamp]])   extends OptionalRTCol2CompareSyntax[R, Timestamp](thisCol)
}
