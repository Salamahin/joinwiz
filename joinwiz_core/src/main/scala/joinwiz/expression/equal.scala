package joinwiz.expression

import joinwiz.Expr.expr
import joinwiz.{Expr, LTCol2, RTCol2}

final case class LTCol2W[O, T] private[joinwiz] (wrapped: LTCol2[O, T])
final case class RTCol2W[O, T] private[joinwiz] (wrapped: RTCol2[O, T])
final case class LTCol2OptW[O, T] private[joinwiz] (wrapped: LTCol2[O, Option[T]])
final case class RTCol2OptW[O, T] private[joinwiz] (wrapped: RTCol2[O, Option[T]])

trait LowLevelEqualSyntax {
  import org.apache.spark.sql.functions.lit

  implicit def ltColToWrapper[O, T](col: LTCol2[O, T]): LTCol2W[O, T]               = LTCol2W[O, T](col)
  implicit def rtColToWrapper[O, T](col: RTCol2[O, T]): RTCol2W[O, T]               = RTCol2W[O, T](col)
  implicit def ltOptColToWrapper[O, T](col: LTCol2[O, Option[T]]): LTCol2OptW[O, T] = LTCol2OptW[O, T](col)
  implicit def rtOptColToWrapper[O, T](col: RTCol2[O, Option[T]]): RTCol2OptW[O, T] = RTCol2OptW[O, T](col)

  implicit class LTCol2EqualSyntax[L, T](thisCol: LTCol2[L, T]) {
    def =:=[R](thatCol: LTCol2W[L, T]): Expr[L, R]    = expr[L, R](thisCol.column === thatCol.wrapped.column)((l, _) => thisCol(l) == thatCol.wrapped(l))
    def =:=[R](thatCol: RTCol2W[R, T]): Expr[L, R]    = expr[L, R](thisCol.column === thatCol.wrapped.column)((l, r) => thisCol(l) == thatCol.wrapped(r))
    def =:=[R](thatCol: LTCol2OptW[L, T]): Expr[L, R] = expr[L, R](thisCol.column === thatCol.wrapped.column)((l, _) => thatCol.wrapped(l) contains thisCol(l))
    def =:=[R](thatCol: RTCol2OptW[R, T]): Expr[L, R] = expr[L, R](thisCol.column === thatCol.wrapped.column)((l, r) => thatCol.wrapped(r) contains thisCol(l))
    def =:=[R](const: T): Expr[L, R]          = expr[L, R](thisCol.column === lit(const))((l, _) => thisCol(l) == const)
  }

  implicit class RTCol2EqualSyntax[R, T](thisCol: RTCol2[R, T]) {
    def =:=[L](thatCol: LTCol2W[L, T]): Expr[L, R]    = expr[L, R](thisCol.column === thatCol.wrapped.column)((l, r) => thisCol(r) == thatCol.wrapped(l))
    def =:=[L](thatCol: RTCol2W[R, T]): Expr[L, R]    = expr[L, R](thisCol.column === thatCol.wrapped.column)((_, r) => thisCol(r) == thatCol.wrapped(r))
    def =:=[L](thatCol: RTCol2OptW[R, T]): Expr[L, R] = expr[L, R](thisCol.column === thatCol.wrapped.column)((_, r) => thatCol.wrapped(r) contains thisCol(r))
    def =:=[L](thatCol: LTCol2OptW[L, T]): Expr[L, R] = expr[L, R](thisCol.column === thatCol.wrapped.column)((l, r) => thatCol.wrapped(l) contains thisCol(r))
    def =:=[L](const: T): Expr[L, R]          = expr[L, R](thisCol.column === lit(const))((_, r) => thisCol(r) == const)
  }
}

trait EqualSyntax extends LowLevelEqualSyntax {
  import org.apache.spark.sql.functions.lit

  implicit class OptionalLTCol2EqualSyntax[L, T](thisCol: LTCol2[L, Option[T]]) {
    def =:=[R](thatCol: LTCol2W[L, T]): Expr[L, R]    = expr[L, R](thisCol.column === thatCol.wrapped.column)((l, _) => thisCol(l) contains thatCol.wrapped(l))
    def =:=[R](thatCol: RTCol2W[R, T]): Expr[L, R]    = expr[L, R](thisCol.column === thatCol.wrapped.column)((l, r) => thisCol(l) contains thatCol.wrapped(r))
    def =:=[R](thatCol: LTCol2OptW[L, T]): Expr[L, R] = expr[L, R](thisCol.column === thatCol.wrapped.column)((l, _) => thisCol.wrapped(l).isDefined && thatCol.wrapped(l) == thisCol.wrapped(l))
    def =:=[R](thatCol: RTCol2OptW[R, T]): Expr[L, R] = expr[L, R](thisCol.column === thatCol.wrapped.column)((l, r) => thatCol.wrapped(r).isDefined && thatCol.wrapped(r) == thisCol.wrapped(l))
    def =:=[R](const: T): Expr[L, R]          = expr[L, R](thisCol.column === lit(const))((l, _) => thisCol(l) contains const)
  }

  implicit class OptionalRTCol2EqualOptionSyntax[R, T](thisCol: RTCol2[R, Option[T]]) {
    def =:=[L](thatCol: LTCol2W[L, T]): Expr[L, R]    = expr[L, R](thisCol.column === thatCol.wrapped.column)((l, r) => thisCol(r) contains thatCol.wrapped(l))
    def =:=[L](thatCol: RTCol2W[R, T]): Expr[L, R]    = expr[L, R](thisCol.column === thatCol.wrapped.column)((_, r) => thisCol(r) contains thatCol.wrapped(r))
    def =:=[L](thatCol: RTCol2OptW[R, T]): Expr[L, R] = expr[L, R](thisCol.column === thatCol.wrapped.column)((_, r) => thatCol.wrapped(r) == thisCol.wrapped(r))
    def =:=[L](thatCol: LTCol2OptW[L, T]): Expr[L, R] = expr[L, R](thisCol.column === thatCol.wrapped.column)((l, r) => thatCol.wrapped(l) == thisCol.wrapped(r))
    def =:=[L](const: T): Expr[L, R]          = expr[L, R](thisCol.column === lit(const))((_, r) => thisCol(r) contains const)
  }
}
