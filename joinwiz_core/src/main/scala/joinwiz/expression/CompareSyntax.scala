package joinwiz.expression

import JoinCondition.joinCondition
import joinwiz.{Id, LTColumn, RTColumn, TColumn}

import java.sql.{Date, Timestamp}

trait SparkOrdered[T] {
  def ordering: Ordering[T]
}

trait SparkOrderedInstances {
  protected def of[T](implicit o: Ordering[T]): SparkOrdered[T] = new SparkOrdered[T] {
    val ordering: Ordering[T] = o
  }

  protected implicit val dateOrdering: Ordering[Date] = new Ordering[Date] {
    override def compare(x: Date, y: Date): Int = Ordering.Long.compare(x.getTime, y.getTime)
  }
  protected implicit val timestampOrdering: Ordering[Timestamp] = new Ordering[Timestamp] {
    override def compare(x: Timestamp, y: Timestamp): Int = Ordering.Long.compare(x.getTime, y.getTime)
  }

  implicit val intOrdered: SparkOrdered[Int]               = of
  implicit val longOrdered: SparkOrdered[Long]             = of
  implicit val shortOrdered: SparkOrdered[Short]           = of
  implicit val byteOrdered: SparkOrdered[Byte]             = of
  implicit val floatOrdered: SparkOrdered[Float]           = of
  implicit val doubleOrdered: SparkOrdered[Double]         = of
  implicit val bigDecimalOrdered: SparkOrdered[BigDecimal] = of
  implicit val dateOrdered: SparkOrdered[Date]             = of
  implicit val timestampOrdered: SparkOrdered[Timestamp]   = of
}

// java.time instances are layered in per Spark version (SparkOrderedVersioned): Spark 3+ only,
// since Spark 2.4 has no LocalDate/Instant encoders.
object SparkOrdered extends SparkOrderedVersioned

trait LowLevelCompareSyntax {
  import org.apache.spark.sql.functions.lit

  abstract class BasicColumnCompareSyntax[F[_], L, R, T](thisCol: TColumn[L, R, F[T]])(implicit op: TColumnCompare[F], ord: Ordering[T]) {
    def <(thatCol: LTColumn[L, R, T]): JoinCondition[L, R] = joinCondition[L, R]((l, r) => op.compare(thisCol.value(l, r), thatCol.value(l, r))(-1))(thisCol.toColumn < thatCol.toColumn)
    def <(thatCol: RTColumn[L, R, T]): JoinCondition[L, R] = joinCondition[L, R]((l, r) => op.compare(thisCol.value(l, r), thatCol.value(l, r))(-1))(thisCol.toColumn < thatCol.toColumn)
    def <(thatCol: LTColumnOptW[L, R, T]): JoinCondition[L, R] =
      joinCondition[L, R]((l, r) => op.compare(thisCol.value(l, r), thatCol.wrapped.value(l, r))(-1))(thisCol.toColumn < thatCol.wrapped.toColumn)
    def <(thatCol: RTColumnOptW[L, R, T]): JoinCondition[L, R] =
      joinCondition[L, R]((l, r) => op.compare(thisCol.value(l, r), thatCol.wrapped.value(l, r))(-1))(thisCol.toColumn < thatCol.wrapped.toColumn)
    def <(const: T): JoinCondition[L, R] = joinCondition[L, R]((l, r) => op.compare(thisCol.value(l, r), const)(-1))(thisCol.toColumn < lit(const))

    def <=(thatCol: LTColumn[L, R, T]): JoinCondition[L, R] = joinCondition[L, R]((l, r) => op.compare(thisCol.value(l, r), thatCol.value(l, r))(-1, 0))(thisCol.toColumn <= thatCol.toColumn)
    def <=(thatCol: RTColumn[L, R, T]): JoinCondition[L, R] = joinCondition[L, R]((l, r) => op.compare(thisCol.value(l, r), thatCol.value(l, r))(-1, 0))(thisCol.toColumn <= thatCol.toColumn)
    def <=(thatCol: LTColumnOptW[L, R, T]): JoinCondition[L, R] =
      joinCondition[L, R]((l, r) => op.compare(thisCol.value(l, r), thatCol.wrapped.value(l, r))(-1, 0))(thisCol.toColumn <= thatCol.wrapped.toColumn)
    def <=(thatCol: RTColumnOptW[L, R, T]): JoinCondition[L, R] =
      joinCondition[L, R]((l, r) => op.compare(thisCol.value(l, r), thatCol.wrapped.value(l, r))(-1, 0))(thisCol.toColumn <= thatCol.wrapped.toColumn)
    def <=(const: T): JoinCondition[L, R] = joinCondition[L, R]((l, r) => op.compare(thisCol.value(l, r), const)(-1, 0))(thisCol.toColumn <= lit(const))

    def >(thatCol: LTColumn[L, R, T]): JoinCondition[L, R] = joinCondition[L, R]((l, r) => op.compare(thisCol.value(l, r), thatCol.value(l, r))(1))(thisCol.toColumn > thatCol.toColumn)
    def >(thatCol: RTColumn[L, R, T]): JoinCondition[L, R] = joinCondition[L, R]((l, r) => op.compare(thisCol.value(l, r), thatCol.value(l, r))(1))(thisCol.toColumn > thatCol.toColumn)
    def >(thatCol: LTColumnOptW[L, R, T]): JoinCondition[L, R] =
      joinCondition[L, R]((l, r) => op.compare(thisCol.value(l, r), thatCol.wrapped.value(l, r))(1))(thisCol.toColumn > thatCol.wrapped.toColumn)
    def >(thatCol: RTColumnOptW[L, R, T]): JoinCondition[L, R] =
      joinCondition[L, R]((l, r) => op.compare(thisCol.value(l, r), thatCol.wrapped.value(l, r))(1))(thisCol.toColumn > thatCol.wrapped.toColumn)
    def >(const: T): JoinCondition[L, R] = joinCondition[L, R]((l, r) => op.compare(thisCol.value(l, r), const)(1))(thisCol.toColumn > lit(const))

    def >=(thatCol: LTColumn[L, R, T]): JoinCondition[L, R] = joinCondition[L, R]((l, r) => op.compare(thisCol.value(l, r), thatCol.value(l, r))(1, 0))(thisCol.toColumn >= thatCol.toColumn)
    def >=(thatCol: RTColumn[L, R, T]): JoinCondition[L, R] = joinCondition[L, R]((l, r) => op.compare(thisCol.value(l, r), thatCol.value(l, r))(1, 0))(thisCol.toColumn >= thatCol.toColumn)
    def >=(thatCol: LTColumnOptW[L, R, T]): JoinCondition[L, R] =
      joinCondition[L, R]((l, r) => op.compare(thisCol.value(l, r), thatCol.wrapped.value(l, r))(1, 0))(thisCol.toColumn >= thatCol.wrapped.toColumn)
    def >=(thatCol: RTColumnOptW[L, R, T]): JoinCondition[L, R] =
      joinCondition[L, R]((l, r) => op.compare(thisCol.value(l, r), thatCol.wrapped.value(l, r))(1, 0))(thisCol.toColumn >= thatCol.wrapped.toColumn)
    def >=(const: T): JoinCondition[L, R] = joinCondition[L, R]((l, r) => op.compare(thisCol.value(l, r), const)(1, 0))(thisCol.toColumn >= lit(const))
  }

  implicit class LTColumnCompareSyntax[L, R, T](thisCol: LTColumn[L, R, T])(implicit s: SparkOrdered[T])
      extends BasicColumnCompareSyntax[Id, L, R, T](thisCol)(implicitly[TColumnCompare[Id]], s.ordering)

  implicit class RTColumnCompareSyntax[L, R, T](thisCol: RTColumn[L, R, T])(implicit s: SparkOrdered[T])
      extends BasicColumnCompareSyntax[Id, L, R, T](thisCol)(implicitly[TColumnCompare[Id]], s.ordering)
}

trait CompareSyntax extends LowLevelCompareSyntax {
  implicit class OptionalLTColumnCompareSyntax[L, R, T](thisCol: LTColumn[L, R, Option[T]])(implicit s: SparkOrdered[T])
      extends BasicColumnCompareSyntax[Option, L, R, T](thisCol)(implicitly[TColumnCompare[Option]], s.ordering)

  implicit class OptionalRTColumnCompareSyntax[L, R, T](thisCol: RTColumn[L, R, Option[T]])(implicit s: SparkOrdered[T])
      extends BasicColumnCompareSyntax[Option, L, R, T](thisCol)(implicitly[TColumnCompare[Option]], s.ordering)
}
