package joinwiz

import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.{Window, WindowSpec}

import scala.annotation.tailrec
import scala.language.experimental.macros
import scala.language.higherKinds
import scala.reflect.macros.blackbox

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

sealed trait TCol[O, +T] {
  def column: Column
  def apply(value: O): T
}
trait LTCol[LO, RO, +T] extends TCol[LO, T]
trait RTCol[LO, RO, +T] extends TCol[RO, T]

trait ExtractTColSyntax {
  implicit class BasicLTColExtract[LO, RO, E](val applyLTCol: ApplyLTCol[LO, RO, E]) {
    def apply[T](expr: E => T): LTCol[LO, RO, T] = macro MacroImpl.leftColumn[LO, RO, E, T]
  }

  implicit class BasicRTColExtract[LO, RO, E](val applyRTCol: ApplyRTCol[LO, RO, E]) {
    def apply[T](expr: E => T): RTCol[LO, RO, T] = macro MacroImpl.rightColumn[LO, RO, E, T]
  }

  implicit class OptionLTColExtract[LO, RO, E](val applyLTCol: ApplyLTCol[LO, RO, Option[E]]) {
    def apply[T](expr: E => T): LTCol[LO, RO, Option[T]] = macro MacroImpl.leftOptColumn[LO, RO, E, T]
  }

  implicit class OptionRTColExtract[LO, RO, E](val applyRTCol: ApplyRTCol[LO, RO, Option[E]]) {
    def apply[T](expr: E => T): RTCol[LO, RO, Option[T]] = macro MacroImpl.rightOptColumn[LO, RO, E, T]
  }
}

class ApplyLTCol[LO, RO, E](val names: Seq[String], val orig: LO => E) extends Serializable {
  private[joinwiz] def map[E1](name: String, newOrig: E => E1) = new ApplyLTCol[LO, RO, E1](names :+ name, newOrig compose orig)
}

class ApplyRTCol[LO, RO, E](val names: Seq[String], val orig: RO => E) extends Serializable {
  private[joinwiz] def map[E1](name: String, newOrig: E => E1) = new ApplyRTCol[LO, RO, E1](names :+ name, newOrig compose orig)
}

object ApplyLTCol {
  def apply[L, R] = new ApplyLTCol[L, R, L](names = Left.alias :: Nil, identity)
}

object ApplyRTCol {
  def apply[L, R] = new ApplyRTCol[L, R, R](names = Right.alias :: Nil, identity)
}

private[joinwiz] object Left {
  val alias = "LEFT"
}
private[joinwiz] object Right {
  val alias = "RIGHT"
}

trait TWindow[O, E] {
  def partitionByCols: List[Column]
  def apply(): WindowSpec = /*Window.partitionBy(partitionByCols: _*)*/ ???

  def partitionBy[S](expr: O => S): TWindow[O, (E, S)] = macro MacroImpl.partitionWindowBy[O, E, S]
  def apply(o: O): E
}

class ApplyTWindow[O] extends Serializable {
  def partitionBy[S](expr: O => S): TWindow[O, S] = macro MacroImpl.basicTWindow[O, S]
}

object TWindow {
  def composeOrdering[T](maybePrevOrdering: Option[Ordering[T]], nextOrdering: Ordering[T]): Ordering[T] = maybePrevOrdering match {
    case None => nextOrdering
    case Some(prev) =>
      new Ordering[T] {
        override def compare(x: T, y: T): Int = {
          val compared = prev.compare(x, y)
          if (compared == 0) nextOrdering.compare(x, y)
          else compared
        }
      }
  }
}

private object MacroImpl {

  def basicTWindow[O: c.WeakTypeTag, S: c.WeakTypeTag](c: blackbox.Context)(expr: c.Expr[O => S]): c.Expr[TWindow[O, S]] = {
    import c.universe._

    val origType  = c.weakTypeOf[O]
    val fieldType = c.weakTypeOf[S]
    val fieldName = extractArgName[O, S](c)(expr)

    c.Expr(
      q"""
         new joinwiz.TWindow[$origType, $fieldType] {
            import org.apache.spark.sql.functions.col
            override def partitionByCols = col($fieldName) :: Nil
            override def apply(o: $origType) = $expr(o)
         }
       """
    )
  }

  def partitionWindowBy[O: c.WeakTypeTag, E: c.WeakTypeTag, S: c.WeakTypeTag](c: blackbox.Context)(expr: c.Expr[O => S]): c.Expr[TWindow[O, (E, S)]] = {
    import c.universe._

    val origType  = c.weakTypeOf[O]
    val prevType  = c.weakTypeOf[E]
    val fieldType = c.weakTypeOf[S]
    val fieldName = extractArgName[O, S](c)(expr)

    c.Expr(
      q"""
         new joinwiz.TWindow[$origType, ($prevType, $fieldType)] {
            private val prev = ${c.prefix}

            import org.apache.spark.sql.functions.col
            override def partitionByCols = prev.partitionByCols :+ col($fieldName)
            override def apply(o: $origType) = (prev(o), $expr(o))
         }
       """
    )
  }

  def orderWindowByAsc[O: c.WeakTypeTag, E: c.WeakTypeTag, S](c: blackbox.Context)(expr: c.Expr[O => S]): c.Expr[TWindow[O, E]] = {
    import c.universe._

    val origType       = c.weakTypeOf[O]
    val extractionType = c.weakTypeOf[E]
    val fieldType      = c.weakTypeOf[S]
    val fieldName      = extractArgName[O, S](c)(expr)

//    c.Expr(
//      q"""
//         new joinwiz.TWindow[$origType, $extractionType](${c.prefix}.partitionByCols, ${c.prefix}.orderByCols :+ org.apache.spark.sql.functions.col($fieldName)) {
//            override def apply(o: $origType) = ${c.prefix}.apply(o)
//            override def ordering = {
//              joinwiz.TWindow.composeOrdering(${c.prefix}.ordering, Ordering.by[$origType, $fieldType]($expr))
//            }
//         }
//       """
//    )

    ???
  }

  def orderWindowByDesc[O: c.WeakTypeTag, E: c.WeakTypeTag, S](c: blackbox.Context)(expr: c.Expr[O => S]): c.Expr[TWindow[O, E]] = {
    import c.universe._

    val origType       = c.weakTypeOf[O]
    val extractionType = c.weakTypeOf[E]
    val fieldType      = c.weakTypeOf[S]
    val fieldName      = extractArgName[O, S](c)(expr)

    ???
  }

  def leftColumn[LO: c.WeakTypeTag, RO: c.WeakTypeTag, E: c.WeakTypeTag, T: c.WeakTypeTag](c: blackbox.Context)(expr: c.Expr[E => T]): c.Expr[LTCol[LO, RO, T]] = {
    import c.universe._

    val leftType  = c.weakTypeOf[LO]
    val rightType = c.weakTypeOf[RO]
    val tType     = c.weakTypeOf[T]
    val name      = extractArgName[E, T](c)(expr)

    c.Expr(
      q"""new joinwiz.LTCol[$leftType, $rightType, $tType] {
            import org.apache.spark.sql.functions.col
            override def apply(value: $leftType): $tType = ($expr compose ${c.prefix}.applyLTCol.orig)(value)
            override def column = col((${c.prefix}.applyLTCol.names :+ $name).mkString("."))
          }"""
    )
  }

  def leftOptColumn[LO: c.WeakTypeTag, RO: c.WeakTypeTag, E: c.WeakTypeTag, T: c.WeakTypeTag](c: blackbox.Context)(expr: c.Expr[E => T]): c.Expr[LTCol[LO, RO, Option[T]]] = {
    import c.universe._

    val leftType  = c.weakTypeOf[LO]
    val rightType = c.weakTypeOf[RO]
    val tType     = c.weakTypeOf[T]
    val name      = extractArgName[E, T](c)(expr)

    c.Expr(
      q"""new joinwiz.LTCol[$leftType, $rightType, Option[$tType]] {
            import org.apache.spark.sql.functions.col
            override def apply(value: $leftType): Option[$tType] = ${c.prefix}.applyLTCol.orig(value).map($expr)
            override def column = col((${c.prefix}.applyLTCol.names :+ $name).mkString("."))
          }"""
    )
  }

  def rightColumn[LO: c.WeakTypeTag, RO: c.WeakTypeTag, E: c.WeakTypeTag, T: c.WeakTypeTag](c: blackbox.Context)(expr: c.Expr[E => T]): c.Expr[RTCol[LO, RO, T]] = {
    import c.universe._

    val leftType  = c.weakTypeOf[LO]
    val rightType = c.weakTypeOf[RO]
    val tType     = c.weakTypeOf[T]
    val name      = extractArgName[E, T](c)(expr)

    c.Expr(
      q"""new joinwiz.RTCol[$leftType, $rightType, $tType] {
            import org.apache.spark.sql.functions.col
            override def apply(value: $rightType): $tType = ($expr compose ${c.prefix}.applyRTCol.orig)(value)
            override def column = col((${c.prefix}.applyRTCol.names :+ $name).mkString("."))
          }"""
    )
  }

  def rightOptColumn[LO: c.WeakTypeTag, RO: c.WeakTypeTag, E: c.WeakTypeTag, T: c.WeakTypeTag](c: blackbox.Context)(expr: c.Expr[E => T]): c.Expr[RTCol[LO, RO, Option[T]]] = {
    import c.universe._

    val leftType  = c.weakTypeOf[LO]
    val rightType = c.weakTypeOf[RO]
    val tType     = c.weakTypeOf[T]
    val name      = extractArgName[E, T](c)(expr)

    c.Expr(
      q"""new joinwiz.RTCol[$leftType, $rightType,  Option[$tType]] {
            import org.apache.spark.sql.functions.col
            override def apply(value: $leftType): Option[$tType] = ${c.prefix}.applyRTCol.orig(value).map($expr)
            override def column = col((${c.prefix}.applyRTCol.names :+ $name).mkString("."))
          }"""
    )
  }

  private def extractArgName[E: c.WeakTypeTag, T: c.WeakTypeTag](c: blackbox.Context)(func: c.Expr[E => T]): String = {
    import c.universe._

    @tailrec
    def extract(tree: c.Tree, acc: List[String]): List[String] = {
      tree match {
        case Ident(_)          => acc
        case Select(q, n)      => extract(q, n.decodedName.toString :: acc)
        case Function(_, body) => extract(body, acc)
        case _                 => c.abort(c.enclosingPosition, s"Unsupported expression: $func, apply should be used for products member selection only")
      }
    }

    extract(func.tree, Nil).mkString(".")
  }
}
