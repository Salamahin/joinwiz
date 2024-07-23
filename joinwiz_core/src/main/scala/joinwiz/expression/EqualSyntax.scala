package joinwiz.expression

import joinwiz.expression.JoinCondition.joinCondition
import joinwiz.{Id, LTColumn, RTColumn}

trait LowLevelEqualSyntax {
  import org.apache.spark.sql.functions.lit

  abstract class BasicLTColEqualSyntax[F[_], L, R, T](thisCol: LTColumn[L, R, F[T]])(implicit op: TColumnCompare[F]) {
    def =:=(thatCol: LTColumn[L, R, T]): JoinCondition[L, R]     = joinCondition[L, R]((l, r) => op.equals(thisCol.value(l, r), thatCol.value(l, r)))(thisCol.toColumn === thatCol.toColumn)
    def =:=(thatCol: RTColumn[L, R, T]): JoinCondition[L, R]     = joinCondition[L, R]((l, r) => op.equals(thisCol.value(l, r), thatCol.value(l, r)))(thisCol.toColumn === thatCol.toColumn)
    def =:=(thatCol: LTColumnOptW[L, R, T]): JoinCondition[L, R] = joinCondition[L, R]((l, r) => op.equals(thisCol.value(l, r), thatCol.wrapped.value(l, r)))(thisCol.toColumn === thatCol.wrapped.toColumn)
    def =:=(thatCol: RTColumnOptW[L, R, T]): JoinCondition[L, R] = joinCondition[L, R]((l, r) => op.equals(thisCol.value(l, r), thatCol.wrapped.value(l, r)))(thisCol.toColumn === thatCol.wrapped.toColumn)
    def =:=(const: T): JoinCondition[L, R]                       = joinCondition[L, R]((l, r) => op.equals(thisCol.value(l, r), const))(thisCol.toColumn === lit(const))
  }

  abstract class BasicRTColEqualSyntax[F[_], L, R, T](thisCol: RTColumn[L, R, F[T]])(implicit op: TColumnCompare[F]) {
    def =:=(thatCol: LTColumn[L, R, T]): JoinCondition[L, R]     = joinCondition[L, R]((l, r) => op.equals(thisCol.value(l, r), thatCol.value(l, r)))(thisCol.toColumn === thatCol.toColumn)
    def =:=(thatCol: RTColumn[L, R, T]): JoinCondition[L, R]     = joinCondition[L, R]((l, r) => op.equals(thisCol.value(l, r), thatCol.value(l, r)))(thisCol.toColumn === thatCol.toColumn)
    def =:=(thatCol: LTColumnOptW[L, R, T]): JoinCondition[L, R] = joinCondition[L, R]((l, r) => op.equals(thisCol.value(l, r), thatCol.wrapped.value(l, r)))(thisCol.toColumn === thatCol.wrapped.toColumn)
    def =:=(thatCol: RTColumnOptW[L, R, T]): JoinCondition[L, R] = joinCondition[L, R]((l, r) => op.equals(thisCol.value(l, r), thatCol.wrapped.value(l, r)))(thisCol.toColumn === thatCol.wrapped.toColumn)
    def =:=(const: T): JoinCondition[L, R]                       = joinCondition[L, R]((l, r) => op.equals(thisCol.value(l, r), const))(thisCol.toColumn === lit(const))
  }

  implicit class LTColEqualSyntax[L, R, T](thisCol: LTColumn[L, R, T]) extends BasicLTColEqualSyntax[Id, L, R, T](thisCol)
  implicit class RTColEqualSyntax[L, R, T](thisCol: RTColumn[L, R, T]) extends BasicRTColEqualSyntax[Id, L, R, T](thisCol)
}

trait EqualSyntax extends LowLevelEqualSyntax {
  implicit class OptionalLTColEqualSyntax[L, R, T](thisCol: LTColumn[L, R, Option[T]]) extends BasicLTColEqualSyntax[Option, L, R, T](thisCol)
  implicit class OptionalRTColEqualSyntax[L, R, T](thisCol: RTColumn[L, R, Option[T]]) extends BasicRTColEqualSyntax[Option, L, R, T](thisCol)
}
