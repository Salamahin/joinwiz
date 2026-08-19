package joinwiz

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

import scala.language.experimental.macros
import scala.reflect.macros.whitebox

trait JoinColumn[LEFT, RIGHT, +T] {
  def value(l: LEFT, r: RIGHT): T
  def toColumn: Column
}

class LeftColumn[LEFT, RIGHT, +T](val path: Seq[String], val get: LEFT => T) extends JoinColumn[LEFT, RIGHT, T] {
  def value(l: LEFT, r: RIGHT): T = get(l)
  def toColumn: Column            = col(path.mkString("."))
}

class RightColumn[LEFT, RIGHT, +T](val path: Seq[String], val get: RIGHT => T) extends JoinColumn[LEFT, RIGHT, T] {
  def value(l: LEFT, r: RIGHT): T = get(r)
  def toColumn: Column            = col(path.mkString("."))
}

object JoinColumn {
  def left[L, R]: LeftColumn[L, R, L]  = new LeftColumn(alias.left :: Nil, identity)
  def right[L, R]: RightColumn[L, R, R] = new RightColumn(alias.right :: Nil, identity)
}

trait ApplyJoinColumnSyntax {
  implicit class ApplyLeftColumnSyntax[LEFT, RIGHT, T](val ltCol: LeftColumn[LEFT, RIGHT, T]) {
    def apply[E](expr: T => E): LeftColumn[LEFT, RIGHT, E] = macro Macro2Impl.leftColumn[LEFT, RIGHT, T, E]
    def >>[E](expr: T => E): LeftColumn[LEFT, RIGHT, E] = macro Macro2Impl.leftColumn[LEFT, RIGHT, T, E]
  }

  implicit class ApplyLeftColumnOptSyntax[LEFT, RIGHT, T](val ltCol: LeftColumn[LEFT, RIGHT, Option[T]]) {
    def apply[E](expr: T => E): LeftColumn[LEFT, RIGHT, Option[E]] = macro Macro2Impl.leftColumnOpt[LEFT, RIGHT, T, E]
    def >>[E](expr: T => E): LeftColumn[LEFT, RIGHT, Option[E]] = macro Macro2Impl.leftColumnOpt[LEFT, RIGHT, T, E]

    def apply[E](expr: T => Option[E]): LeftColumn[LEFT, RIGHT, Option[E]] = macro Macro2Impl.leftColumnOptFlatten[LEFT, RIGHT, T, E]
    def >>[E](expr: T => Option[E]): LeftColumn[LEFT, RIGHT, Option[E]] = macro Macro2Impl.leftColumnOptFlatten[LEFT, RIGHT, T, E]
  }

  implicit class ApplyRightColumnSyntax[LEFT, RIGHT, T](val rtCol: RightColumn[LEFT, RIGHT, T]) {
    def apply[E](expr: T => E): RightColumn[LEFT, RIGHT, E] = macro Macro2Impl.rightColumn[LEFT, RIGHT, T, E]
    def >>[E](expr: T => E): RightColumn[LEFT, RIGHT, E] = macro Macro2Impl.rightColumn[LEFT, RIGHT, T, E]
  }

  implicit class ApplyRightColumnOptSyntax[LEFT, RIGHT, T](val rtCol: RightColumn[LEFT, RIGHT, Option[T]]) {
    def apply[E](expr: T => E): RightColumn[LEFT, RIGHT, Option[E]] = macro Macro2Impl.rightColumnOpt[LEFT, RIGHT, T, E]
    def >>[E](expr: T => E): RightColumn[LEFT, RIGHT, Option[E]] = macro Macro2Impl.rightColumnOpt[LEFT, RIGHT, T, E]

    def apply[E](expr: T => Option[E]): RightColumn[LEFT, RIGHT, Option[E]] = macro Macro2Impl.rightColumnOptFlatten[LEFT, RIGHT, T, E]
    def >>[E](expr: T => Option[E]): RightColumn[LEFT, RIGHT, Option[E]] = macro Macro2Impl.rightColumnOptFlatten[LEFT, RIGHT, T, E]
  }
}

object Macro2Impl {

  def leftColumn[LEFT: c.WeakTypeTag, RIGHT: c.WeakTypeTag, T: c.WeakTypeTag, E: c.WeakTypeTag](c: whitebox.Context)(expr: c.Expr[T => E]): c.Expr[LeftColumn[LEFT, RIGHT, E]] = {
    import c.universe._
    val ltCol = q"${c.prefix}.ltCol"
    c.Expr(q"new joinwiz.LeftColumn[${weakTypeOf[LEFT]}, ${weakTypeOf[RIGHT]}, ${weakTypeOf[E]}](path = $ltCol.path :+ ${argName(c)(expr)}, get = $ltCol.get.andThen($expr))")
  }

  def leftColumnOpt[LEFT: c.WeakTypeTag, RIGHT: c.WeakTypeTag, T: c.WeakTypeTag, E: c.WeakTypeTag](c: whitebox.Context)(expr: c.Expr[T => E]): c.Expr[LeftColumn[LEFT, RIGHT, Option[E]]] = {
    import c.universe._
    val ltCol = q"${c.prefix}.ltCol"
    c.Expr(q"new joinwiz.LeftColumn[${weakTypeOf[LEFT]}, ${weakTypeOf[RIGHT]}, Option[${weakTypeOf[E]}]](path = $ltCol.path :+ ${argName(c)(expr)}, get = $ltCol.get.andThen(_.map($expr)))")
  }

  def leftColumnOptFlatten[LEFT: c.WeakTypeTag, RIGHT: c.WeakTypeTag, T: c.WeakTypeTag, E: c.WeakTypeTag](
    c: whitebox.Context
  )(expr: c.Expr[T => Option[E]]): c.Expr[LeftColumn[LEFT, RIGHT, Option[E]]] = {
    import c.universe._
    val ltCol = q"${c.prefix}.ltCol"
    c.Expr(q"new joinwiz.LeftColumn[${weakTypeOf[LEFT]}, ${weakTypeOf[RIGHT]}, Option[${weakTypeOf[E]}]](path = $ltCol.path :+ ${argName(c)(expr)}, get = $ltCol.get.andThen(_.flatMap($expr)))")
  }

  def rightColumn[LEFT: c.WeakTypeTag, RIGHT: c.WeakTypeTag, T: c.WeakTypeTag, E: c.WeakTypeTag](c: whitebox.Context)(expr: c.Expr[T => E]): c.Expr[RightColumn[LEFT, RIGHT, E]] = {
    import c.universe._
    val rtCol = q"${c.prefix}.rtCol"
    c.Expr(q"new joinwiz.RightColumn[${weakTypeOf[LEFT]}, ${weakTypeOf[RIGHT]}, ${weakTypeOf[E]}](path = $rtCol.path :+ ${argName(c)(expr)}, get = $rtCol.get.andThen($expr))")
  }

  def rightColumnOpt[LEFT: c.WeakTypeTag, RIGHT: c.WeakTypeTag, T: c.WeakTypeTag, E: c.WeakTypeTag](c: whitebox.Context)(expr: c.Expr[T => E]): c.Expr[RightColumn[LEFT, RIGHT, Option[E]]] = {
    import c.universe._
    val rtCol = q"${c.prefix}.rtCol"
    c.Expr(q"new joinwiz.RightColumn[${weakTypeOf[LEFT]}, ${weakTypeOf[RIGHT]}, Option[${weakTypeOf[E]}]](path = $rtCol.path :+ ${argName(c)(expr)}, get = $rtCol.get.andThen(_.map($expr)))")
  }

  def rightColumnOptFlatten[LEFT: c.WeakTypeTag, RIGHT: c.WeakTypeTag, T: c.WeakTypeTag, E: c.WeakTypeTag](
    c: whitebox.Context
  )(expr: c.Expr[T => Option[E]]): c.Expr[RightColumn[LEFT, RIGHT, Option[E]]] = {
    import c.universe._
    val rtCol = q"${c.prefix}.rtCol"
    c.Expr(q"new joinwiz.RightColumn[${weakTypeOf[LEFT]}, ${weakTypeOf[RIGHT]}, Option[${weakTypeOf[E]}]](path = $rtCol.path :+ ${argName(c)(expr)}, get = $rtCol.get.andThen(_.flatMap($expr)))")
  }
}
