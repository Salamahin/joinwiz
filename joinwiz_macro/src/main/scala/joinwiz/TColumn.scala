package joinwiz

import scala.reflect.macros.whitebox
import scala.language.experimental.macros

class LTColumn[LEFT, RIGHT, +T](val path: Seq[String], val get: LEFT => T)
class RTColumn[LEFT, RIGHT, +T](val path: Seq[String], val get: RIGHT => T)

object TColumn {
  def left[T]: LTColumn[T, T, T]  = new LTColumn(alias.left :: Nil, identity)
  def right[T]: RTColumn[T, T, T] = new RTColumn(alias.right :: Nil, identity)
}

trait ApplyTColumnSyntax {
  implicit class ApplyLTColumnSyntax[LEFT, RIGHT, T](val ltCol: LTColumn[LEFT, RIGHT, T]) {
    def apply[E](expr: T => E): LTColumn[LEFT, RIGHT, E] = macro TColumnMacroImpl.leftColumn[LEFT, RIGHT, T, E]
    def >>[E](expr: T => E): LTColumn[LEFT, RIGHT, E] = macro TColumnMacroImpl.leftColumn[LEFT, RIGHT, T, E]
  }

  implicit class ApplyLTColumnOptSyntax[LEFT, RIGHT, T](val ltCol: LTColumn[LEFT, RIGHT, Option[T]]) {
    def apply[E](expr: T => E): LTColumn[LEFT, RIGHT, Option[E]] = macro TColumnMacroImpl.leftColumnOpt[LEFT, RIGHT, T, E]
    def >>[E](expr: T => E): LTColumn[LEFT, RIGHT, Option[E]] = macro TColumnMacroImpl.leftColumnOpt[LEFT, RIGHT, T, E]

    def apply[E](expr: T => Option[E]): LTColumn[LEFT, RIGHT, Option[E]] = macro TColumnMacroImpl.leftColumnOptFlatten[LEFT, RIGHT, T, E]
    def >>[E](expr: T => Option[E]): LTColumn[LEFT, RIGHT, Option[E]] = macro TColumnMacroImpl.leftColumnOptFlatten[LEFT, RIGHT, T, E]
  }

  implicit class ApplyRTColumnSyntax[LEFT, RIGHT, T](val rtCol: RTColumn[LEFT, RIGHT, T]) {
    def apply[E](expr: T => E): RTColumn[LEFT, RIGHT, E] = macro TColumnMacroImpl.rightColumn[LEFT, RIGHT, T, E]
    def >>[E](expr: T => E): RTColumn[LEFT, RIGHT, E] = macro TColumnMacroImpl.rightColumn[LEFT, RIGHT, T, E]
  }

  implicit class ApplyRTColumnOptSyntax[LEFT, RIGHT, T](val rtCol: RTColumn[LEFT, RIGHT, Option[T]]) {
    def apply[E](expr: T => E): RTColumn[LEFT, RIGHT, Option[E]] = macro TColumnMacroImpl.rightColumnOpt[LEFT, RIGHT, T, E]
    def >>[E](expr: T => E): RTColumn[LEFT, RIGHT, Option[E]] = macro TColumnMacroImpl.rightColumnOpt[LEFT, RIGHT, T, E]

    def apply[E](expr: T => Option[E]): RTColumn[LEFT, RIGHT, Option[E]] = macro TColumnMacroImpl.rightColumnOptFlatten[LEFT, RIGHT, T, E]
    def >>[E](expr: T => Option[E]): RTColumn[LEFT, RIGHT, Option[E]] = macro TColumnMacroImpl.rightColumnOptFlatten[LEFT, RIGHT, T, E]
  }
}

object TColumnMacroImpl {
  def leftColumn[LEFT: c.WeakTypeTag, RIGHT: c.WeakTypeTag, T: c.WeakTypeTag, E: c.WeakTypeTag](c: whitebox.Context)(expr: c.Expr[T => E]): c.Expr[LTColumn[LEFT, RIGHT, E]] = {
    import c.universe._

    val leftType  = c.weakTypeOf[LEFT]
    val rightType = c.weakTypeOf[RIGHT]
    val eType     = c.weakTypeOf[E]
    val name      = argName(c)(expr)

    c.Expr(
      q"""
         new joinwiz.LTColumn[$leftType, $rightType, $eType](
           path = ${c.prefix}.ltCol.path :+ $name,
           get = ${c.prefix}.ltCol.get andThen $expr
         )
         """
    )
  }

  def leftColumnOpt[LEFT: c.WeakTypeTag, RIGHT: c.WeakTypeTag, T: c.WeakTypeTag, E: c.WeakTypeTag](c: whitebox.Context)(expr: c.Expr[T => E]): c.Expr[LTColumn[LEFT, RIGHT, Option[E]]] = {
    import c.universe._

    val leftType  = c.weakTypeOf[LEFT]
    val rightType = c.weakTypeOf[RIGHT]
    val eType     = c.weakTypeOf[E]
    val name      = argName(c)(expr)

    c.Expr(
      q"""
         new joinwiz.LTColumn[$leftType, $rightType, Option[$eType]](
           path = ${c.prefix}.ltCol.path :+ $name,
           get = ${c.prefix}.ltCol.get.andThen(_.map($expr))
         )
         """
    )
  }

  def leftColumnOptFlatten[LEFT: c.WeakTypeTag, RIGHT: c.WeakTypeTag, T: c.WeakTypeTag, E: c.WeakTypeTag](
    c: whitebox.Context
  )(expr: c.Expr[T => Option[E]]): c.Expr[LTColumn[LEFT, RIGHT, Option[E]]] = {
    import c.universe._

    val leftType  = c.weakTypeOf[LEFT]
    val rightType = c.weakTypeOf[RIGHT]
    val eType     = c.weakTypeOf[E]
    val name      = argName(c)(expr)

    c.Expr(
      q"""
         new joinwiz.LTColumn[$leftType, $rightType, Option[$eType]](
           path = ${c.prefix}.ltCol.path :+ $name,
           get = ${c.prefix}.ltCol.get.andThen(_.flatMap($expr))
         )
         """
    )
  }

  def rightColumn[LEFT: c.WeakTypeTag, RIGHT: c.WeakTypeTag, T: c.WeakTypeTag, E: c.WeakTypeTag](c: whitebox.Context)(expr: c.Expr[T => E]): c.Expr[RTColumn[LEFT, RIGHT, E]] = {
    import c.universe._

    val leftType  = c.weakTypeOf[LEFT]
    val rightType = c.weakTypeOf[RIGHT]
    val eType     = c.weakTypeOf[E]
    val name      = argName(c)(expr)

    c.Expr(
      q"""
         new joinwiz.RTColumn[$leftType, $rightType, $eType](
           path = ${c.prefix}.rtCol.path :+ $name,
           get = ${c.prefix}.rtCol.get andThen $expr
         )
         """
    )
  }

  def rightColumnOpt[LEFT: c.WeakTypeTag, RIGHT: c.WeakTypeTag, T: c.WeakTypeTag, E: c.WeakTypeTag](c: whitebox.Context)(expr: c.Expr[T => E]): c.Expr[RTColumn[LEFT, RIGHT, Option[E]]] = {
    import c.universe._

    val leftType  = c.weakTypeOf[LEFT]
    val rightType = c.weakTypeOf[RIGHT]
    val eType     = c.weakTypeOf[E]
    val name      = argName(c)(expr)

    c.Expr(
      q"""
         new joinwiz.RTColumn[$leftType, $rightType, Option[$eType]](
           path = ${c.prefix}.rtCol.path :+ $name,
           get = ${c.prefix}.rtCol.get.andThen(_.map($expr))
         )
         """
    )
  }

  def rightColumnOptFlatten[LEFT: c.WeakTypeTag, RIGHT: c.WeakTypeTag, T: c.WeakTypeTag, E: c.WeakTypeTag](
    c: whitebox.Context
  )(expr: c.Expr[T => Option[E]]): c.Expr[RTColumn[LEFT, RIGHT, Option[E]]] = {
    import c.universe._

    val leftType  = c.weakTypeOf[LEFT]
    val rightType = c.weakTypeOf[RIGHT]
    val eType     = c.weakTypeOf[E]
    val name      = argName(c)(expr)

    c.Expr(
      q"""
         new joinwiz.RTColumn[$leftType, $rightType, Option[$eType]](
           path = ${c.prefix}.rtCol.path :+ $name,
           get = ${c.prefix}.rtCol.get.andThen(_.flatMap($expr))
         )
         """
    )
  }
}
