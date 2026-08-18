package joinwiz

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

import scala.language.experimental.macros
import scala.reflect.macros.whitebox

/**
  * Typed reference to a column of a single relation `T`, projected down to element type `A`.
  * Unlike [[TColumn]] (which is join-scoped and carries a `LEFT`/`RIGHT` alias), this addresses the
  * dataset directly, so `toColumn` is an un-aliased `col(path)` that Catalyst can push to the source.
  */
class FColumn[T, +A](val path: Seq[String], val get: T => A) {
  def value(t: T): A   = get(t)
  def toColumn: Column = col(path.mkString("."))
}

object FColumn {
  def apply[T]: FColumn[T, T] = new FColumn(Nil, identity)
}

trait ApplyFColumnSyntax {
  implicit class ApplyFColumnOps[T, A](val fCol: FColumn[T, A]) {
    def apply[E](expr: A => E): FColumn[T, E] = macro FColumnMacro.select[T, A, E]
    def >>[E](expr: A => E): FColumn[T, E] = macro FColumnMacro.select[T, A, E]
  }
}

object FColumnMacro {
  def select[T: c.WeakTypeTag, A: c.WeakTypeTag, E: c.WeakTypeTag](c: whitebox.Context)(expr: c.Expr[A => E]): c.Expr[FColumn[T, E]] = {
    import c.universe._
    val fCol = q"${c.prefix}.fCol"
    c.Expr(q"new joinwiz.FColumn[${weakTypeOf[T]}, ${weakTypeOf[E]}](path = $fCol.path :+ ${argName(c)(expr)}, get = $fCol.get.andThen($expr))")
  }
}
