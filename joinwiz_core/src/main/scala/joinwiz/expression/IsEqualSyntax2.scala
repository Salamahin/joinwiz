package joinwiz.expression

import joinwiz.expression.JoinCondition.joinCondition
import joinwiz.{Id, LTColumn, RTColumn}
import org.apache.spark.sql.functions.lit

trait TColumnEquals[F[_]] {
  def areEqual[T](thisFt: F[T], thatT: T): Boolean
  def areEqual[T](thisFt: F[T], thatOptT: Option[T]): Boolean
}

object TColumnEquals {
  implicit val tColumnEqualToOption: TColumnEquals[Option] = new TColumnEquals[Option] {
    override def areEqual[T](thisFt: Option[T], thatT: T): Boolean            = thisFt contains thatT
    override def areEqual[T](thisFt: Option[T], thatOptT: Option[T]): Boolean = thisFt.isDefined && thisFt == thatOptT
  }

  implicit val tColumEqualToId: TColumnEquals[Id] = new TColumnEquals[Id] {
    override def areEqual[T](thisFt: Id[T], thatT: T): Boolean            = thisFt == thatT
    override def areEqual[T](thisFt: Id[T], thatOptT: Option[T]): Boolean = thatOptT contains thisFt
  }
}

trait IsEqualSyntax2 {
  abstract class BasicLTColEqualSyntax[F[_], L, R, T](thisCol: LTColumn[L, R, F[T]])(implicit op: TColumnEquals[F]) {
    def =:=(thatCol: LTColumn[L, R, T]): JoinCondition[L, R]  = joinCondition[L, R]((l, _) => op.areEqual(thisCol.value(l), thatCol.value(l)))(thisCol.toColumn === thatCol.toColumn)
    def =:=(thatCol: RTColumn[L, R, T]): JoinCondition[L, R]  = joinCondition[L, R]((l, r) => op.areEqual(thisCol.value(l), thatCol.value(r)))(thisCol.toColumn === thatCol.toColumn)
    def =:=(thatCol: LTColOptW[L, R, T]): JoinCondition[L, R] = joinCondition[L, R]((l, _) => op.areEqual(thisCol.value(l), thatCol.wrapped(l)))(thisCol.toColumn === thatCol.wrapped.column)
    def =:=(thatCol: RTColOptW[L, R, T]): JoinCondition[L, R] = joinCondition[L, R]((l, r) => op.areEqual(thisCol.value(l), thatCol.wrapped(r)))(thisCol.toColumn === thatCol.wrapped.column)
    def =:=(const: T): JoinCondition[L, R]                    = joinCondition[L, R]((l, _) => op.equals(thisCol.value(l), const))(thisCol.toColumn === lit(const))
  }

  abstract class BasicRTColEqualSyntax[F[_], L, R, T](thisCol: RTColumn[L, R, F[T]])(implicit op: TColumnEquals[F]) {
    def =:=(thatCol: LTColumn[L, R, T]): JoinCondition[L, R]  = joinCondition[L, R]((l, r) => op.areEqual(thisCol.value(r), thatCol.value(l)))(thisCol.toColumn === thatCol.toColumn)
    def =:=(thatCol: RTColumn[L, R, T]): JoinCondition[L, R]  = joinCondition[L, R]((_, r) => op.areEqual(thisCol.value(r), thatCol.value(r)))(thisCol.toColumn === thatCol.toColumn)
    def =:=(thatCol: LTColOptW[L, R, T]): JoinCondition[L, R] = joinCondition[L, R]((l, r) => op.areEqual(thisCol.value(r), thatCol.wrapped(l)))(thisCol.toColumn === thatCol.wrapped.column)
    def =:=(thatCol: RTColOptW[L, R, T]): JoinCondition[L, R] = joinCondition[L, R]((_, r) => op.areEqual(thisCol.value(r), thatCol.wrapped(r)))(thisCol.toColumn === thatCol.wrapped.column)
    def =:=(const: T): JoinCondition[L, R]                    = joinCondition[L, R]((_, r) => op.equals(thisCol.value(r), const))(thisCol.toColumn === lit(const))
  }

  implicit class LTColEqualSyntax[L, R, T](thisCol: LTColumn[L, R, T]) extends BasicLTColEqualSyntax[Id, L, R, T](thisCol)
  implicit class RTColEqualSyntax[L, R, T](thisCol: RTColumn[L, R, T]) extends BasicRTColEqualSyntax[Id, L, R, T](thisCol)
}
