package joinwiz.expression

import joinwiz.expression.FilterCondition.filterCondition
import joinwiz.{FColumn, Id}

/** `Option`-typed single-relation column wrapped so it can be compared against a non-optional one. */
final case class FColumnOptW[T, A] private[joinwiz] (wrapped: FColumn[T, Option[A]])

trait FilterWrappers {
  implicit def fOptColToWrapper[T, A](col: FColumn[T, Option[A]]): FColumnOptW[T, A] = FColumnOptW[T, A](col)
}

trait FilterCombinatorsSyntax {
  implicit class CombineFilterSyntax[T](thisF: FilterCondition[T]) {
    // OR is safe in a filter (it lowers to a source `Or` filter); only a join OR forces a nested-loop join.
    def &&(thatF: FilterCondition[T]): FilterCondition[T] = filterCondition[T](t => thisF(t) && thatF(t))(thisF() && thatF())
    def ||(thatF: FilterCondition[T]): FilterCondition[T] = filterCondition[T](t => thisF(t) || thatF(t))(thisF() || thatF())
  }
}

trait LowLevelFilterEqualSyntax {
  import org.apache.spark.sql.functions.lit

  abstract class BasicFColEqualSyntax[F[_], T, A](thisCol: FColumn[T, F[A]])(implicit op: TColumnCompare[F]) {
    def =:=(that: FColumn[T, A]): FilterCondition[T] = filterCondition[T](t => op.equals(thisCol.value(t), that.value(t)))(thisCol.toColumn === that.toColumn)
    def =:=(that: FColumnOptW[T, A]): FilterCondition[T] =
      filterCondition[T](t => op.equals(thisCol.value(t), that.wrapped.value(t)))(thisCol.toColumn === that.wrapped.toColumn)
    def =:=(const: A): FilterCondition[T] = filterCondition[T](t => op.equals(thisCol.value(t), const))(thisCol.toColumn === lit(const))
  }

  implicit class FColEqualSyntax[T, A](thisCol: FColumn[T, A]) extends BasicFColEqualSyntax[Id, T, A](thisCol)
}

trait FilterEqualSyntax extends LowLevelFilterEqualSyntax {
  implicit class OptionalFColEqualSyntax[T, A](thisCol: FColumn[T, Option[A]]) extends BasicFColEqualSyntax[Option, T, A](thisCol)
}

trait LowLevelFilterCompareSyntax {
  import org.apache.spark.sql.functions.lit

  abstract class BasicFColCompareSyntax[F[_], T, A](thisCol: FColumn[T, F[A]])(implicit op: TColumnCompare[F], ord: Ordering[A]) {
    def <(that: FColumn[T, A]): FilterCondition[T] = filterCondition[T](t => op.compare(thisCol.value(t), that.value(t))(-1))(thisCol.toColumn < that.toColumn)
    def <(that: FColumnOptW[T, A]): FilterCondition[T] =
      filterCondition[T](t => op.compare(thisCol.value(t), that.wrapped.value(t))(-1))(thisCol.toColumn < that.wrapped.toColumn)
    def <(const: A): FilterCondition[T] = filterCondition[T](t => op.compare(thisCol.value(t), const)(-1))(thisCol.toColumn < lit(const))

    def <=(that: FColumn[T, A]): FilterCondition[T] = filterCondition[T](t => op.compare(thisCol.value(t), that.value(t))(-1, 0))(thisCol.toColumn <= that.toColumn)
    def <=(that: FColumnOptW[T, A]): FilterCondition[T] =
      filterCondition[T](t => op.compare(thisCol.value(t), that.wrapped.value(t))(-1, 0))(thisCol.toColumn <= that.wrapped.toColumn)
    def <=(const: A): FilterCondition[T] = filterCondition[T](t => op.compare(thisCol.value(t), const)(-1, 0))(thisCol.toColumn <= lit(const))

    def >(that: FColumn[T, A]): FilterCondition[T] = filterCondition[T](t => op.compare(thisCol.value(t), that.value(t))(1))(thisCol.toColumn > that.toColumn)
    def >(that: FColumnOptW[T, A]): FilterCondition[T] =
      filterCondition[T](t => op.compare(thisCol.value(t), that.wrapped.value(t))(1))(thisCol.toColumn > that.wrapped.toColumn)
    def >(const: A): FilterCondition[T] = filterCondition[T](t => op.compare(thisCol.value(t), const)(1))(thisCol.toColumn > lit(const))

    def >=(that: FColumn[T, A]): FilterCondition[T] = filterCondition[T](t => op.compare(thisCol.value(t), that.value(t))(1, 0))(thisCol.toColumn >= that.toColumn)
    def >=(that: FColumnOptW[T, A]): FilterCondition[T] =
      filterCondition[T](t => op.compare(thisCol.value(t), that.wrapped.value(t))(1, 0))(thisCol.toColumn >= that.wrapped.toColumn)
    def >=(const: A): FilterCondition[T] = filterCondition[T](t => op.compare(thisCol.value(t), const)(1, 0))(thisCol.toColumn >= lit(const))
  }

  implicit class FColCompareSyntax[T, A](thisCol: FColumn[T, A])(implicit s: SparkOrdered[A])
      extends BasicFColCompareSyntax[Id, T, A](thisCol)(implicitly[TColumnCompare[Id]], s.ordering)
}

trait FilterCompareSyntax extends LowLevelFilterCompareSyntax {
  implicit class OptionalFColCompareSyntax[T, A](thisCol: FColumn[T, Option[A]])(implicit s: SparkOrdered[A])
      extends BasicFColCompareSyntax[Option, T, A](thisCol)(implicitly[TColumnCompare[Option]], s.ordering)
}
