package joinwiz

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

import scala.language.experimental.macros
import scala.reflect.macros.whitebox

class FilterColumn[T, +A](val path: Seq[String], val get: T => A) {
  def value(t: T): A   = get(t)
  def toColumn: Column = col(path.mkString("."))
}

object FilterColumn {
  def apply[T]: FilterColumn[T, T] = new FilterColumn(Nil, identity)
}

trait ApplyFilterColumnSyntax {
  implicit class ApplyFilterColumnOps[T, A](val fCol: FilterColumn[T, A]) {
    def apply[E](expr: A => E): FilterColumn[T, E] = macro FilterColumnMacro.select[T, A, E]
    def >>[E](expr: A => E): FilterColumn[T, E] = macro FilterColumnMacro.select[T, A, E]
  }
}

object FilterColumnMacro {
  def select[T: c.WeakTypeTag, A: c.WeakTypeTag, E: c.WeakTypeTag](c: whitebox.Context)(expr: c.Expr[A => E]): c.Expr[FilterColumn[T, E]] = {
    import c.universe._
    val fCol = q"${c.prefix}.fCol"
    c.Expr(q"new joinwiz.FilterColumn[${weakTypeOf[T]}, ${weakTypeOf[E]}](path = $fCol.path :+ ${argName(c)(expr)}, get = $fCol.get.andThen($expr))")
  }
}
