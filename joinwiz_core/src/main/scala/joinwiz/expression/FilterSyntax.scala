package joinwiz.expression

import joinwiz.expression.FilterCondition.filterCondition
import joinwiz.{FilterColumn, Id}

final case class FilterColumnOptW[T, A] private[joinwiz] (wrapped: FilterColumn[T, Option[A]])

trait FilterWrappers {
  implicit def fOptColToWrapper[T, A](col: FilterColumn[T, Option[A]]): FilterColumnOptW[T, A] = FilterColumnOptW[T, A](col)
}

trait FilterCombinatorsSyntax {
  implicit class CombineFilterSyntax[T](thisF: FilterCondition[T]) {
    def &&(thatF: FilterCondition[T]): FilterCondition[T] = filterCondition[T](t => thisF(t) && thatF(t))(thisF() && thatF())
    def ||(thatF: FilterCondition[T]): FilterCondition[T] = filterCondition[T](t => thisF(t) || thatF(t))(thisF() || thatF())
  }
}

trait LowLevelFilterEqualSyntax {
  import org.apache.spark.sql.functions.lit

  abstract class BasicFColEqualSyntax[F[_], T, A](thisCol: FilterColumn[T, F[A]])(implicit op: ColumnCompare[F]) {
    def =:=(that: FilterColumn[T, A]): FilterCondition[T] = filterCondition[T](t => op.equals(thisCol.value(t), that.value(t)))(thisCol.toColumn === that.toColumn)
    def =:=(that: FilterColumnOptW[T, A]): FilterCondition[T] =
      filterCondition[T](t => op.equals(thisCol.value(t), that.wrapped.value(t)))(thisCol.toColumn === that.wrapped.toColumn)
    def =:=(const: A): FilterCondition[T] = filterCondition[T](t => op.equals(thisCol.value(t), const))(thisCol.toColumn === lit(const))
  }

  implicit class FColEqualSyntax[T, A](thisCol: FilterColumn[T, A]) extends BasicFColEqualSyntax[Id, T, A](thisCol)
}

trait FilterEqualSyntax extends LowLevelFilterEqualSyntax {
  implicit class OptionalFColEqualSyntax[T, A](thisCol: FilterColumn[T, Option[A]]) extends BasicFColEqualSyntax[Option, T, A](thisCol)
}

trait LowLevelFilterCompareSyntax {
  import org.apache.spark.sql.functions.lit

  abstract class BasicFColCompareSyntax[F[_], T, A](thisCol: FilterColumn[T, F[A]])(implicit op: ColumnCompare[F], ord: Ordering[A]) {
    def <(that: FilterColumn[T, A]): FilterCondition[T] = filterCondition[T](t => op.compare(thisCol.value(t), that.value(t))(-1))(thisCol.toColumn < that.toColumn)
    def <(that: FilterColumnOptW[T, A]): FilterCondition[T] =
      filterCondition[T](t => op.compare(thisCol.value(t), that.wrapped.value(t))(-1))(thisCol.toColumn < that.wrapped.toColumn)
    def <(const: A): FilterCondition[T] = filterCondition[T](t => op.compare(thisCol.value(t), const)(-1))(thisCol.toColumn < lit(const))

    def <=(that: FilterColumn[T, A]): FilterCondition[T] = filterCondition[T](t => op.compare(thisCol.value(t), that.value(t))(-1, 0))(thisCol.toColumn <= that.toColumn)
    def <=(that: FilterColumnOptW[T, A]): FilterCondition[T] =
      filterCondition[T](t => op.compare(thisCol.value(t), that.wrapped.value(t))(-1, 0))(thisCol.toColumn <= that.wrapped.toColumn)
    def <=(const: A): FilterCondition[T] = filterCondition[T](t => op.compare(thisCol.value(t), const)(-1, 0))(thisCol.toColumn <= lit(const))

    def >(that: FilterColumn[T, A]): FilterCondition[T] = filterCondition[T](t => op.compare(thisCol.value(t), that.value(t))(1))(thisCol.toColumn > that.toColumn)
    def >(that: FilterColumnOptW[T, A]): FilterCondition[T] =
      filterCondition[T](t => op.compare(thisCol.value(t), that.wrapped.value(t))(1))(thisCol.toColumn > that.wrapped.toColumn)
    def >(const: A): FilterCondition[T] = filterCondition[T](t => op.compare(thisCol.value(t), const)(1))(thisCol.toColumn > lit(const))

    def >=(that: FilterColumn[T, A]): FilterCondition[T] = filterCondition[T](t => op.compare(thisCol.value(t), that.value(t))(1, 0))(thisCol.toColumn >= that.toColumn)
    def >=(that: FilterColumnOptW[T, A]): FilterCondition[T] =
      filterCondition[T](t => op.compare(thisCol.value(t), that.wrapped.value(t))(1, 0))(thisCol.toColumn >= that.wrapped.toColumn)
    def >=(const: A): FilterCondition[T] = filterCondition[T](t => op.compare(thisCol.value(t), const)(1, 0))(thisCol.toColumn >= lit(const))
  }

  implicit class FColCompareSyntax[T, A](thisCol: FilterColumn[T, A])(implicit s: SparkOrdered[A])
      extends BasicFColCompareSyntax[Id, T, A](thisCol)(implicitly[ColumnCompare[Id]], s.ordering)
}

trait FilterCompareSyntax extends LowLevelFilterCompareSyntax {
  implicit class OptionalFColCompareSyntax[T, A](thisCol: FilterColumn[T, Option[A]])(implicit s: SparkOrdered[A])
      extends BasicFColCompareSyntax[Option, T, A](thisCol)(implicitly[ColumnCompare[Option]], s.ordering)
}
