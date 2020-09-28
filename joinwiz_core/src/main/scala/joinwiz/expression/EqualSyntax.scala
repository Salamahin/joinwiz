package joinwiz.expression

import joinwiz.Expr.expr
import joinwiz.expression.TColOps.Id
import joinwiz.{Expr, LTCol, RTCol}

trait LowLevelEqualSyntax {
  import org.apache.spark.sql.functions.lit

  abstract class BasicLTColEqualSyntax[F[_], L, R, T](thisCol: LTCol[L, R, F[T]])(implicit op: TColOps[F]) {
    def =:=(thatCol: LTCol[L, R, T]): Expr[L, R]     = expr[L, R](thisCol.column === thatCol.column)((l, _) => op.equalsToSame(thisCol(l), thatCol(l)))
    def =:=(thatCol: RTCol[L, R, T]): Expr[L, R]     = expr[L, R](thisCol.column === thatCol.column)((l, r) => op.equalsToSame(thisCol(l), thatCol(r)))
    def =:=(thatCol: LTColOptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column === thatCol.wrapped.column)((l, _) => op.equalsToOption(thisCol(l), thatCol.wrapped(l)))
    def =:=(thatCol: RTColOptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column === thatCol.wrapped.column)((l, r) => op.equalsToOption(thisCol(l), thatCol.wrapped(r)))
    def =:=(const: T): Expr[L, R]                    = expr[L, R](thisCol.column === lit(const))((l, _) => op.equalsToSame(thisCol(l), const))
  }

  abstract class BasicRTColEqualSyntax[F[_], L, R, T](thisCol: RTCol[L, R, F[T]])(implicit op: TColOps[F]) {
    def =:=(thatCol: LTColW[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column === thatCol.wrapped.column)((l, r) => op.equalsToSame(thisCol(r), thatCol.wrapped(l)))
    def =:=(thatCol: RTColW[L, R, T]): Expr[L, R]    = expr[L, R](thisCol.column === thatCol.wrapped.column)((_, r) => op.equalsToSame(thisCol(r), thatCol.wrapped(r)))
    def =:=(thatCol: LTColOptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column === thatCol.wrapped.column)((l, r) => op.equalsToOption(thisCol(r), thatCol.wrapped(l)))
    def =:=(thatCol: RTColOptW[L, R, T]): Expr[L, R] = expr[L, R](thisCol.column === thatCol.wrapped.column)((_, r) => op.equalsToOption(thisCol(r), thatCol.wrapped(r)))
    def =:=(const: T): Expr[L, R]                    = expr[L, R](thisCol.column === lit(const))((_, r) => op.equalsToSame(thisCol(r), const))
  }

  implicit class LTColEqualSyntax[L, R, T](thisCol: LTCol[L, R, T]) extends BasicLTColEqualSyntax[Id, L, R, T](thisCol)
  implicit class RTColEqualSyntax[L, R, T](thisCol: RTCol[L, R, T]) extends BasicRTColEqualSyntax[Id, L, R, T](thisCol)
}

trait EqualSyntax extends LowLevelEqualSyntax {
  implicit class OptionalLTColEqualSyntax[L, R, T](thisCol: LTCol[L, R, Option[T]]) extends BasicLTColEqualSyntax[Option, L, R, T](thisCol)
  implicit class OptionalRTColEqualSyntax[L, R, T](thisCol: RTCol[L, R, Option[T]]) extends BasicRTColEqualSyntax[Option, L, R, T](thisCol)
}
