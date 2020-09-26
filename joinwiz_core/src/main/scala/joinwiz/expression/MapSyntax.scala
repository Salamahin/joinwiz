package joinwiz.expression

import joinwiz.{Expr, LTCol, RTCol}
import org.apache.spark.sql.Column

trait MapSyntax {
  implicit class LTColMapSyntax[L, R, T](thisCol: LTCol[L, R, T]) {
    def map[U](forTestKit: T => U)(forSpark: Column => Column): LTCol[L, R, U] = new LTCol[L, R, U] {
      override def column: Column     = forSpark(thisCol.column)
      override def apply(value: L): U = (forTestKit compose thisCol.apply)(value)
    }
  }

  implicit class RTColMapSyntax[L, R, T](thisCol: RTCol[L, R, T]) {
    def map[U](forTestKit: T => U)(forSpark: Column => Column): RTCol[L, R, U] = new RTCol[L, R, U] {
      override def column: Column     = forSpark(thisCol.column)
      override def apply(value: R): U = (forTestKit compose thisCol.apply)(value)
    }
  }

  implicit class LTColExprSyntax[L, R, T](thisCol: LTCol[L, R, Boolean]) {
    val expr: Expr[L, R] = Expr.expr(thisCol.column)((l, _) => thisCol(l))
  }

  implicit class RTColExprSyntax[L, R, T](thisCol: RTCol[L, R, Boolean]) {
    val expr: Expr[L, R] = Expr.expr(thisCol.column)((_, r) => thisCol(r))
  }
}

