package joinwiz.expression

import joinwiz.expression.TColOps.Id
import joinwiz.{Expr, LTCol, RTCol}
import org.apache.spark.sql.Column

trait LowLevelMapSyntax {
  abstract class BasicLTColMapSyntax[F[_], L, R, T](thisCol: LTCol[L, R, F[T]])(implicit f: TColOps[F]) {
    def map[U](forTestKit: T => U)(forSpark: Column => Column): LTCol[L, R, F[U]] = new LTCol[L, R, F[U]] {
      override def column: Column        = forSpark(thisCol.column)
      override def apply(value: L): F[U] = f.map(thisCol(value))(forTestKit)
    }
  }

  abstract class BasicRTColMapSyntax[F[_], L, R, T](thisCol: RTCol[L, R, F[T]])(implicit f: TColOps[F]) {
    def map[U](forTestKit: T => U)(forSpark: Column => Column): RTCol[L, R, F[U]] = new RTCol[L, R, F[U]] {
      override def column: Column        = forSpark(thisCol.column)
      override def apply(value: R): F[U] = f.map(thisCol(value))(forTestKit)
    }
  }

  implicit class IdLTColMapSyntax[L, R, T](thisCol: LTCol[L, R, Id[T]]) extends BasicLTColMapSyntax[Id, L, R, T](thisCol)
  implicit class IdRTColMapSyntax[L, R, T](thisCol: RTCol[L, R, Id[T]]) extends BasicRTColMapSyntax[Id, L, R, T](thisCol)

  implicit class LTColExprSyntax[L, R, T](thisCol: LTCol[L, R, Boolean]) {
    val expr: Expr[L, R] = Expr.expr(thisCol.column)((l, _) => thisCol(l))
  }

  implicit class RTColExprSyntax[L, R, T](thisCol: RTCol[L, R, Boolean]) {
    val expr: Expr[L, R] = Expr.expr(thisCol.column)((_, r) => thisCol(r))
  }
}

trait MapSyntax extends LowLevelMapSyntax {
  implicit class OptionLTColMapSyntax[L, R, T](thisCol: LTCol[L, R, Option[T]]) extends BasicLTColMapSyntax[Option, L, R, T](thisCol)
  implicit class OptionRTColMapSyntax[L, R, T](thisCol: RTCol[L, R, Option[T]]) extends BasicRTColMapSyntax[Option, L, R, T](thisCol)
}
