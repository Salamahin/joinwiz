package joinwiz.expression

import org.apache.spark.sql.Column

trait Expr[L, R] {
  def apply(): Column
  def apply(left: L, right: R): Boolean
}

object Expr {
  def expr[L, R](f: (L, R) => Boolean)(c: Column): Expr[L, R] = new Expr[L, R] {
    override def apply(): Column                   = c
    override def apply(left: L, right: R): Boolean = f(left, right)
  }
}
