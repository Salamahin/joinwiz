package joinwiz.expression

import joinwiz.Expr.expr
import joinwiz.{Expr, LTCol, RTCol}

trait LowLevelEqualSyntax {
  import org.apache.spark.sql.functions.lit

  implicit class LTColEqualSyntax[L, R, T](thisCol: LTCol[L, R, T]) {
    def =:=(thatCol: LTColW[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column === thatCol.wrapped.column)((l, _) => thisCol(l) == thatCol.wrapped(l))
    def =:=(thatCol: RTColW[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column === thatCol.wrapped.column)((l, r) => thisCol(l) == thatCol.wrapped(r))
    def =:=(thatCol: LTColOptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column === thatCol.wrapped.column)((l, _) => thatCol.wrapped(l) contains thisCol(l))
    def =:=(thatCol: RTColOptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column === thatCol.wrapped.column)((l, r) => thatCol.wrapped(r) contains thisCol(l))
    def =:=(const: T): Expr[L, R]                     = expr[L, R](thisCol.column === lit(const))((l, _) => thisCol(l) == const)
  }

  implicit class RTColEqualSyntax[L, R, T](thisCol: RTCol[L, R, T]) {
    def =:=(thatCol: LTColW[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column === thatCol.wrapped.column)((l, r) => thisCol(r) == thatCol.wrapped(l))
    def =:=(thatCol: RTColW[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column === thatCol.wrapped.column)((_, r) => thisCol(r) == thatCol.wrapped(r))
    def =:=(thatCol: LTColOptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column === thatCol.wrapped.column)((l, r) => thatCol.wrapped(l) contains thisCol(r))
    def =:=(thatCol: RTColOptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column === thatCol.wrapped.column)((_, r) => thatCol.wrapped(r) contains thisCol(r))
    def =:=(const: T): Expr[L, R]                     = expr[L, R](thisCol.column === lit(const))((_, r) => thisCol(r) == const)
  }
}

trait EqualSyntax extends LowLevelEqualSyntax {
  import org.apache.spark.sql.functions.lit

  implicit class OptionalLTColEqualSyntax[L, R, T](thisCol: LTCol[L, R, Option[T]]) {
    def =:=(thatCol: LTColW[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column === thatCol.wrapped.column)((l, _) => thisCol(l) contains thatCol.wrapped(l))
    def =:=(thatCol: RTColW[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column === thatCol.wrapped.column)((l, r) => thisCol(l) contains thatCol.wrapped(r))
    def =:=(thatCol: LTColOptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column === thatCol.wrapped.column)((l, _) => thisCol(l).isDefined && thatCol.wrapped(l) == thisCol(l))
    def =:=(thatCol: RTColOptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column === thatCol.wrapped.column)((l, r) => thisCol(l).isDefined && thatCol.wrapped(r) == thisCol(l))
    def =:=(const: T): Expr[L, R]                     = expr[L, R](thisCol.column === lit(const))((l, _) => thisCol(l) contains const)
  }

  implicit class OptionalRTColEqualSyntax[L, R, T](thisCol: RTCol[L, R, Option[T]]) {
    def =:=(thatCol: LTColW[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column === thatCol.wrapped.column)((l, r) => thisCol(r) contains thatCol.wrapped(l))
    def =:=(thatCol: RTColW[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column === thatCol.wrapped.column)((_, r) => thisCol(r) contains thatCol.wrapped(r))
    def =:=(thatCol: LTColOptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column === thatCol.wrapped.column)((l, r) => thisCol(r).isDefined && thatCol.wrapped(l) == thisCol(r))
    def =:=(thatCol: RTColOptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column === thatCol.wrapped.column)((_, r) => thisCol(r).isDefined && thatCol.wrapped(r) == thisCol(r))
    def =:=(const: T): Expr[L, R]                     = expr[L, R](thisCol.column === lit(const))((_, r) => thisCol(r) contains const)
  }
}
