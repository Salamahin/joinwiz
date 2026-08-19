package joinwiz.expression

import joinwiz.expression.JoinCondition.joinCondition
import joinwiz.{Id, LeftColumn, RightColumn, JoinColumn}

trait LowLevelEqualSyntax {
  import org.apache.spark.sql.functions.lit

  abstract class BasicColEqualSyntax[F[_], L, R, T](thisCol: JoinColumn[L, R, F[T]])(implicit op: ColumnCompare[F]) {
    def =:=(thatCol: LeftColumn[L, R, T]): JoinCondition[L, R] = joinCondition[L, R]((l, r) => op.equals(thisCol.value(l, r), thatCol.value(l, r)))(thisCol.toColumn === thatCol.toColumn)
    def =:=(thatCol: RightColumn[L, R, T]): JoinCondition[L, R] = joinCondition[L, R]((l, r) => op.equals(thisCol.value(l, r), thatCol.value(l, r)))(thisCol.toColumn === thatCol.toColumn)
    def =:=(thatCol: LeftColumnOptW[L, R, T]): JoinCondition[L, R] =
      joinCondition[L, R]((l, r) => op.equals(thisCol.value(l, r), thatCol.wrapped.value(l, r)))(thisCol.toColumn === thatCol.wrapped.toColumn)
    def =:=(thatCol: RightColumnOptW[L, R, T]): JoinCondition[L, R] =
      joinCondition[L, R]((l, r) => op.equals(thisCol.value(l, r), thatCol.wrapped.value(l, r)))(thisCol.toColumn === thatCol.wrapped.toColumn)
    def =:=(const: T): JoinCondition[L, R] = joinCondition[L, R]((l, r) => op.equals(thisCol.value(l, r), const))(thisCol.toColumn === lit(const))
  }

  implicit class LTColEqualSyntax[L, R, T](thisCol: LeftColumn[L, R, T]) extends BasicColEqualSyntax[Id, L, R, T](thisCol)
  implicit class RTColEqualSyntax[L, R, T](thisCol: RightColumn[L, R, T]) extends BasicColEqualSyntax[Id, L, R, T](thisCol)
}

trait EqualSyntax extends LowLevelEqualSyntax {
  implicit class OptionalLTColEqualSyntax[L, R, T](thisCol: LeftColumn[L, R, Option[T]]) extends BasicColEqualSyntax[Option, L, R, T](thisCol)
  implicit class OptionalRTColEqualSyntax[L, R, T](thisCol: RightColumn[L, R, Option[T]]) extends BasicColEqualSyntax[Option, L, R, T](thisCol)
}
