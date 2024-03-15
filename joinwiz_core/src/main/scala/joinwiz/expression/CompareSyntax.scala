package joinwiz.expression

import JoinCondition.joinCondition
import joinwiz.{Id, LTColumn, RTColumn}

import java.sql.{Date, Timestamp}

trait LowLevelCompareSyntax {
  import org.apache.spark.sql.functions.lit

  implicit val dateOrdering: Ordering[Date]           = new Ordering[Date] {
    override def compare(x: Date, y: Date): Int = implicitly[Ordering[Long]].compare(x.getTime, y.getTime)
  }
  implicit val timestampOrdering: Ordering[Timestamp] = new Ordering[Timestamp] {
    override def compare(x: Timestamp, y: Timestamp): Int = implicitly[Ordering[Long]].compare(x.getTime, y.getTime)
  }

  abstract class BasicLTColumnCompareSyntax[F[_], L, R, T: Ordering](thisCol: LTColumn[L, R, F[T]])(implicit op: TColumnCompare[F]) {
    def <(thatCol: LTColumn[L, R, T]): JoinCondition[L, R]     = joinCondition[L, R]((l, _) => op.compare(thisCol.value(l), thatCol.value(l))(-1))(thisCol.toColumn < thatCol.toColumn)
    def <(thatCol: RTColumn[L, R, T]): JoinCondition[L, R]     = joinCondition[L, R]((l, r) => op.compare(thisCol.value(l), thatCol.value(r))(-1))(thisCol.toColumn < thatCol.toColumn)
    def <(thatCol: LTColumnOptW[L, R, T]): JoinCondition[L, R] = joinCondition[L, R]((l, _) => op.compare(thisCol.value(l), thatCol.wrapped.value(l))(-1))(thisCol.toColumn < thatCol.wrapped.toColumn)
    def <(thatCol: RTColumnOptW[L, R, T]): JoinCondition[L, R] = joinCondition[L, R]((l, r) => op.compare(thisCol.value(l), thatCol.wrapped.value(r))(-1))(thisCol.toColumn < thatCol.wrapped.toColumn)
    def <(const: T): JoinCondition[L, R]                    = joinCondition[L, R]((l, _) => op.compare(thisCol.value(l), const)(-1))(thisCol.toColumn < lit(const))

    def <=(thatCol: LTColumn[L, R, T]): JoinCondition[L, R]     = joinCondition[L, R]((l, _) => op.compare(thisCol.value(l), thatCol.value(l))(-1, 0))(thisCol.toColumn <= thatCol.toColumn)
    def <=(thatCol: RTColumn[L, R, T]): JoinCondition[L, R]     = joinCondition[L, R]((l, r) => op.compare(thisCol.value(l), thatCol.value(r))(-1, 0))(thisCol.toColumn <= thatCol.toColumn)
    def <=(thatCol: LTColumnOptW[L, R, T]): JoinCondition[L, R] = joinCondition[L, R]((l, _) => op.compare(thisCol.value(l), thatCol.wrapped.value(l))(-1, 0))(thisCol.toColumn <= thatCol.wrapped.toColumn)
    def <=(thatCol: RTColumnOptW[L, R, T]): JoinCondition[L, R] = joinCondition[L, R]((l, r) => op.compare(thisCol.value(l), thatCol.wrapped.value(r))(-1, 0))(thisCol.toColumn <= thatCol.wrapped.toColumn)
    def <=(const: T): JoinCondition[L, R]                    = joinCondition[L, R]((l, _) => op.compare(thisCol.value(l), const)(-1, 0))(thisCol.toColumn <= lit(const))

    def >(thatCol: LTColumn[L, R, T]): JoinCondition[L, R]     = joinCondition[L, R]((l, _) => op.compare(thisCol.value(l), thatCol.value(l))(1))(thisCol.toColumn > thatCol.toColumn)
    def >(thatCol: RTColumn[L, R, T]): JoinCondition[L, R]     = joinCondition[L, R]((l, r) => op.compare(thisCol.value(l), thatCol.value(r))(1))(thisCol.toColumn > thatCol.toColumn)
    def >(thatCol: LTColumnOptW[L, R, T]): JoinCondition[L, R] = joinCondition[L, R]((l, _) => op.compare(thisCol.value(l), thatCol.wrapped.value(l))(1))(thisCol.toColumn > thatCol.wrapped.toColumn)
    def >(thatCol: RTColumnOptW[L, R, T]): JoinCondition[L, R] = joinCondition[L, R]((l, r) => op.compare(thisCol.value(l), thatCol.wrapped.value(r))(1))(thisCol.toColumn > thatCol.wrapped.toColumn)
    def >(const: T): JoinCondition[L, R]                    = joinCondition[L, R]((l, _) => op.compare(thisCol.value(l), const)(1))(thisCol.toColumn > lit(const))

    def >=(thatCol: LTColumn[L, R, T]): JoinCondition[L, R]     = joinCondition[L, R]((l, _) => op.compare(thisCol.value(l), thatCol.value(l))(1, 0))(thisCol.toColumn >= thatCol.toColumn)
    def >=(thatCol: RTColumn[L, R, T]): JoinCondition[L, R]     = joinCondition[L, R]((l, r) => op.compare(thisCol.value(l), thatCol.value(r))(1, 0))(thisCol.toColumn >= thatCol.toColumn)
    def >=(thatCol: LTColumnOptW[L, R, T]): JoinCondition[L, R] = joinCondition[L, R]((l, _) => op.compare(thisCol.value(l), thatCol.wrapped.value(l))(1, 0))(thisCol.toColumn >= thatCol.wrapped.toColumn)
    def >=(thatCol: RTColumnOptW[L, R, T]): JoinCondition[L, R] = joinCondition[L, R]((l, r) => op.compare(thisCol.value(l), thatCol.wrapped.value(r))(1, 0))(thisCol.toColumn >= thatCol.wrapped.toColumn)
    def >=(const: T): JoinCondition[L, R]                    = joinCondition[L, R]((l, _) => op.compare(thisCol.value(l), const)(1, 0))(thisCol.toColumn >= lit(const))
  }

  abstract class BasicRTColumnCompareSyntax[F[_], L, R, T: Ordering](thisCol: RTColumn[L, R, F[T]])(implicit op: TColumnCompare[F]) {
    def <(thatCol: LTColumn[L, R, T]): JoinCondition[L, R]     = joinCondition[L, R]((l, r) => op.compare(thisCol.value(r), thatCol.value(l))(-1))(thisCol.toColumn < thatCol.toColumn)
    def <(thatCol: RTColumn[L, R, T]): JoinCondition[L, R]     = joinCondition[L, R]((_, r) => op.compare(thisCol.value(r), thatCol.value(r))(-1))(thisCol.toColumn < thatCol.toColumn)
    def <(thatCol: LTColumnOptW[L, R, T]): JoinCondition[L, R] = joinCondition[L, R]((l, r) => op.compare(thisCol.value(r), thatCol.wrapped.value(l))(-1))(thisCol.toColumn < thatCol.wrapped.toColumn)
    def <(thatCol: RTColumnOptW[L, R, T]): JoinCondition[L, R] = joinCondition[L, R]((_, r) => op.compare(thisCol.value(r), thatCol.wrapped.value(r))(-1))(thisCol.toColumn < thatCol.wrapped.toColumn)
    def <(const: T): JoinCondition[L, R]                    = joinCondition[L, R]((_, r) => op.compare(thisCol.value(r), const)(-1))(thisCol.toColumn < lit(const))

    def <=(thatCol: LTColumn[L, R, T]): JoinCondition[L, R]     = joinCondition[L, R]((l, r) => op.compare(thisCol.value(r), thatCol.value(l))(-1, 0))(thisCol.toColumn <= thatCol.toColumn)
    def <=(thatCol: RTColumn[L, R, T]): JoinCondition[L, R]     = joinCondition[L, R]((_, r) => op.compare(thisCol.value(r), thatCol.value(r))(-1, 0))(thisCol.toColumn <= thatCol.toColumn)
    def <=(thatCol: LTColumnOptW[L, R, T]): JoinCondition[L, R] = joinCondition[L, R]((l, r) => op.compare(thisCol.value(r), thatCol.wrapped.value(l))(-1, 0))(thisCol.toColumn <= thatCol.wrapped.toColumn)
    def <=(thatCol: RTColumnOptW[L, R, T]): JoinCondition[L, R] = joinCondition[L, R]((_, r) => op.compare(thisCol.value(r), thatCol.wrapped.value(r))(-1, 0))(thisCol.toColumn <= thatCol.wrapped.toColumn)
    def <=(const: T): JoinCondition[L, R]                    = joinCondition[L, R]((_, r) => op.compare(thisCol.value(r), const)(-1, 0))(thisCol.toColumn <= lit(const))

    def >(thatCol: LTColumn[L, R, T]): JoinCondition[L, R]     = joinCondition[L, R]((l, r) => op.compare(thisCol.value(r), thatCol.value(l))(1))(thisCol.toColumn > thatCol.toColumn)
    def >(thatCol: RTColumn[L, R, T]): JoinCondition[L, R]     = joinCondition[L, R]((_, r) => op.compare(thisCol.value(r), thatCol.value(r))(1))(thisCol.toColumn > thatCol.toColumn)
    def >(thatCol: LTColumnOptW[L, R, T]): JoinCondition[L, R] = joinCondition[L, R]((l, r) => op.compare(thisCol.value(r), thatCol.wrapped.value(l))(1))(thisCol.toColumn > thatCol.wrapped.toColumn)
    def >(thatCol: RTColumnOptW[L, R, T]): JoinCondition[L, R] = joinCondition[L, R]((_, r) => op.compare(thisCol.value(r), thatCol.wrapped.value(r))(1))(thisCol.toColumn > thatCol.wrapped.toColumn)
    def >(const: T): JoinCondition[L, R]                    = joinCondition[L, R]((_, r) => op.compare(thisCol.value(r), const)(1))(thisCol.toColumn > lit(const))

    def >=(thatCol: LTColumn[L, R, T]): JoinCondition[L, R]     = joinCondition[L, R]((l, r) => op.compare(thisCol.value(r), thatCol.value(l))(1, 0))(thisCol.toColumn >= thatCol.toColumn)
    def >=(thatCol: RTColumn[L, R, T]): JoinCondition[L, R]     = joinCondition[L, R]((_, r) => op.compare(thisCol.value(r), thatCol.value(r))(1, 0))(thisCol.toColumn >= thatCol.toColumn)
    def >=(thatCol: LTColumnOptW[L, R, T]): JoinCondition[L, R] = joinCondition[L, R]((l, r) => op.compare(thisCol.value(r), thatCol.wrapped.value(l))(1, 0))(thisCol.toColumn >= thatCol.wrapped.toColumn)
    def >=(thatCol: RTColumnOptW[L, R, T]): JoinCondition[L, R] = joinCondition[L, R]((_, r) => op.compare(thisCol.value(r), thatCol.wrapped.value(r))(1, 0))(thisCol.toColumn >= thatCol.wrapped.toColumn)
    def >=(const: T): JoinCondition[L, R]                    = joinCondition[L, R]((_, r) => op.compare(thisCol.value(r), const)(1, 0))(thisCol.toColumn >= lit(const))
  }

  implicit class LTColumnIntCompareSyntax[L, R](thisCol: LTColumn[L, R, Int])               extends BasicLTColumnCompareSyntax[Id, L, R, Int](thisCol)
  implicit class LTColumnLongCompareSyntax[L, R](thisCol: LTColumn[L, R, Long])             extends BasicLTColumnCompareSyntax[Id, L, R, Long](thisCol)
  implicit class LTColumnShortCompareSyntax[L, R](thisCol: LTColumn[L, R, Short])           extends BasicLTColumnCompareSyntax[Id, L, R, Short](thisCol)
  implicit class LTColumnByteCompareSyntax[L, R](thisCol: LTColumn[L, R, Byte])             extends BasicLTColumnCompareSyntax[Id, L, R, Byte](thisCol)
  implicit class LTColumnFloatCompareSyntax[L, R](thisCol: LTColumn[L, R, Float])           extends BasicLTColumnCompareSyntax[Id, L, R, Float](thisCol)
  implicit class LTColumnDoubleCompareSyntax[L, R](thisCol: LTColumn[L, R, Double])         extends BasicLTColumnCompareSyntax[Id, L, R, Double](thisCol)
  implicit class LTColumnBigDecimalCompareSyntax[L, R](thisCol: LTColumn[L, R, BigDecimal]) extends BasicLTColumnCompareSyntax[Id, L, R, BigDecimal](thisCol)
  implicit class LTColumnDateCompareSyntax[L, R](thisCol: LTColumn[L, R, Date])             extends BasicLTColumnCompareSyntax[Id, L, R, Date](thisCol)
  implicit class LTColumnTimestampCompareSyntax[L, R](thisCol: LTColumn[L, R, Timestamp])   extends BasicLTColumnCompareSyntax[Id, L, R, Timestamp](thisCol)

  implicit class RTColumnIntCompareSyntax[L, R](thisCol: RTColumn[L, R, Int])               extends BasicRTColumnCompareSyntax[Id, L, R, Int](thisCol)
  implicit class RTColumnLongCompareSyntax[L, R](thisCol: RTColumn[L, R, Long])             extends BasicRTColumnCompareSyntax[Id, L, R, Long](thisCol)
  implicit class RTColumnShortCompareSyntax[L, R](thisCol: RTColumn[L, R, Short])           extends BasicRTColumnCompareSyntax[Id, L, R, Short](thisCol)
  implicit class RTColumnByteCompareSyntax[L, R](thisCol: RTColumn[L, R, Byte])             extends BasicRTColumnCompareSyntax[Id, L, R, Byte](thisCol)
  implicit class RTColumnFloatCompareSyntax[L, R](thisCol: RTColumn[L, R, Float])           extends BasicRTColumnCompareSyntax[Id, L, R, Float](thisCol)
  implicit class RTColumnDoubleCompareSyntax[L, R](thisCol: RTColumn[L, R, Double])         extends BasicRTColumnCompareSyntax[Id, L, R, Double](thisCol)
  implicit class RTColumnBigDecimalCompareSyntax[L, R](thisCol: RTColumn[L, R, BigDecimal]) extends BasicRTColumnCompareSyntax[Id, L, R, BigDecimal](thisCol)
  implicit class RTColumnDateCompareSyntax[L, R](thisCol: RTColumn[L, R, Date])             extends BasicRTColumnCompareSyntax[Id, L, R, Date](thisCol)
  implicit class RTColumnTimestampCompareSyntax[L, R](thisCol: RTColumn[L, R, Timestamp])   extends BasicRTColumnCompareSyntax[Id, L, R, Timestamp](thisCol)
}

trait CompareSyntax extends LowLevelCompareSyntax {
  implicit class OptionalLTColumnIntCompareSyntax[L, R](thisCol: LTColumn[L, R, Option[Int]])               extends BasicLTColumnCompareSyntax[Option, L, R, Int](thisCol)
  implicit class OptionalLTColumnLongCompareSyntax[L, R](thisCol: LTColumn[L, R, Option[Long]])             extends BasicLTColumnCompareSyntax[Option, L, R, Long](thisCol)
  implicit class OptionalLTColumnShortCompareSyntax[L, R](thisCol: LTColumn[L, R, Option[Short]])           extends BasicLTColumnCompareSyntax[Option, L, R, Short](thisCol)
  implicit class OptionalLTColumnByteCompareSyntax[L, R](thisCol: LTColumn[L, R, Option[Byte]])             extends BasicLTColumnCompareSyntax[Option, L, R, Byte](thisCol)
  implicit class OptionalLTColumnFloatCompareSyntax[L, R](thisCol: LTColumn[L, R, Option[Float]])           extends BasicLTColumnCompareSyntax[Option, L, R, Float](thisCol)
  implicit class OptionalLTColumnDoubleCompareSyntax[L, R](thisCol: LTColumn[L, R, Option[Double]])         extends BasicLTColumnCompareSyntax[Option, L, R, Double](thisCol)
  implicit class OptionalLTColumnBigDecimalCompareSyntax[L, R](thisCol: LTColumn[L, R, Option[BigDecimal]]) extends BasicLTColumnCompareSyntax[Option, L, R, BigDecimal](thisCol)
  implicit class OptionalLTColumnDateCompareSyntax[L, R](thisCol: LTColumn[L, R, Option[Date]])             extends BasicLTColumnCompareSyntax[Option, L, R, Date](thisCol)
  implicit class OptionalLTColumnTimestampCompareSyntax[L, R](thisCol: LTColumn[L, R, Option[Timestamp]])   extends BasicLTColumnCompareSyntax[Option, L, R, Timestamp](thisCol)

  implicit class OptionalRTColumnIntCompareSyntax[L, R](thisCol: RTColumn[L, R, Option[Int]])               extends BasicRTColumnCompareSyntax[Option, L, R, Int](thisCol)
  implicit class OptionalRTColumnLongCompareSyntax[L, R](thisCol: RTColumn[L, R, Option[Long]])             extends BasicRTColumnCompareSyntax[Option, L, R, Long](thisCol)
  implicit class OptionalRTColumnShortCompareSyntax[L, R](thisCol: RTColumn[L, R, Option[Short]])           extends BasicRTColumnCompareSyntax[Option, L, R, Short](thisCol)
  implicit class OptionalRTColumnByteCompareSyntax[L, R](thisCol: RTColumn[L, R, Option[Byte]])             extends BasicRTColumnCompareSyntax[Option, L, R, Byte](thisCol)
  implicit class OptionalRTColumnFloatCompareSyntax[L, R](thisCol: RTColumn[L, R, Option[Float]])           extends BasicRTColumnCompareSyntax[Option, L, R, Float](thisCol)
  implicit class OptionalRTColumnDoubleCompareSyntax[L, R](thisCol: RTColumn[L, R, Option[Double]])         extends BasicRTColumnCompareSyntax[Option, L, R, Double](thisCol)
  implicit class OptionalRTColumnBigDecimalCompareSyntax[L, R](thisCol: RTColumn[L, R, Option[BigDecimal]]) extends BasicRTColumnCompareSyntax[Option, L, R, BigDecimal](thisCol)
  implicit class OptionalRTColumnDateCompareSyntax[L, R](thisCol: RTColumn[L, R, Option[Date]])             extends BasicRTColumnCompareSyntax[Option, L, R, Date](thisCol)
  implicit class OptionalRTColumnTimestampCompareSyntax[L, R](thisCol: RTColumn[L, R, Option[Timestamp]])   extends BasicRTColumnCompareSyntax[Option, L, R, Timestamp](thisCol)
}
