package joinwiz.expression

import joinwiz.Expr.expr
import joinwiz.{Expr, Id, LTCol, RTCol}

trait LowLevelEqualSyntax {
  import org.apache.spark.sql.functions.lit

  abstract class BasicLTColEqualSyntax[F[_], L, R, T](thisCol: LTCol[L, R, F[T]])(implicit op: TColCompare[F]) {
    def =:=(thatCol: LTCol[L, R, T]): Expr[L, R]     = expr[L, R]((l, _) => op.equals(thisCol(l), thatCol(l)))(thisCol.column === thatCol.column)
    def =:=(thatCol: RTCol[L, R, T]): Expr[L, R]     = expr[L, R]((l, r) => op.equals(thisCol(l), thatCol(r)))(thisCol.column === thatCol.column)
    def =:=(thatCol: LTColOptW[L, R, T]): Expr[L, R] = expr[L, R]((l, _) => op.equals(thisCol(l), thatCol.wrapped(l)))(thisCol.column === thatCol.wrapped.column)
    def =:=(thatCol: RTColOptW[L, R, T]): Expr[L, R] = expr[L, R]((l, r) => op.equals(thisCol(l), thatCol.wrapped(r)))(thisCol.column === thatCol.wrapped.column)
    def =:=(const: T): Expr[L, R]                    = expr[L, R]((l, _) => op.equals(thisCol(l), const))(thisCol.column === lit(const))
  }

  abstract class BasicRTColEqualSyntax[F[_], L, R, T](thisCol: RTCol[L, R, F[T]])(implicit op: TColCompare[F]) {
    def =:=(thatCol: LTCol[L, R, T]): Expr[L, R]    = expr[L, R]((l, r) => op.equals(thisCol(r), thatCol(l)))(thisCol.column === thatCol.column)
    def =:=(thatCol: RTCol[L, R, T]): Expr[L, R]    = expr[L, R]((_, r) => op.equals(thisCol(r), thatCol(r)))(thisCol.column === thatCol.column)
    def =:=(thatCol: LTColOptW[L, R, T]): Expr[L, R] = expr[L, R]((l, r) => op.equals(thisCol(r), thatCol.wrapped(l)))(thisCol.column === thatCol.wrapped.column)
    def =:=(thatCol: RTColOptW[L, R, T]): Expr[L, R] = expr[L, R]((_, r) => op.equals(thisCol(r), thatCol.wrapped(r)))(thisCol.column === thatCol.wrapped.column)
    def =:=(const: T): Expr[L, R]                    = expr[L, R]((_, r) => op.equals(thisCol(r), const))(thisCol.column === lit(const))
  }

  implicit class LTColEqualSyntax[L, R, T](thisCol: LTCol[L, R, T]) extends BasicLTColEqualSyntax[Id, L, R, T](thisCol)
  implicit class RTColEqualSyntax[L, R, T](thisCol: RTCol[L, R, T]) extends BasicRTColEqualSyntax[Id, L, R, T](thisCol)
}

trait EqualSyntax extends LowLevelEqualSyntax {
  implicit class OptionalLTColEqualSyntax[L, R, T](thisCol: LTCol[L, R, Option[T]]) extends BasicLTColEqualSyntax[Option, L, R, T](thisCol)
  implicit class OptionalRTColEqualSyntax[L, R, T](thisCol: RTCol[L, R, Option[T]]) extends BasicRTColEqualSyntax[Option, L, R, T](thisCol)
}
