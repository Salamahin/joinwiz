package joinwiz

import org.apache.spark.sql.Column

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

sealed class ApplyLTCol[LO, RO, E](val names: Seq[String], val orig: LO => E) extends Serializable {
  private[joinwiz] def map[E1](name: String, newOrig: E => E1) =
    new ApplyLTCol[LO, RO, E1](names :+ name, newOrig compose orig)

  def apply[T](expr: E => T): LTCol[LO, RO, T] = macro ApplyCol.leftColumn[LO, RO, E, T]
}

sealed class ApplyRTCol[LO, RO, E](val names: Seq[String], val orig: RO => E) extends Serializable {
  private[joinwiz] def map[E1](name: String, newOrig: E => E1) =
    new ApplyRTCol[LO, RO, E1](names :+ name, newOrig compose orig)

  def apply[T](expr: E => T): RTCol[LO, RO, T] = macro ApplyCol.rightColumn[LO, RO, E, T]
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

private object ApplyCol {

  def leftColumn[LO: c.WeakTypeTag, RO: c.WeakTypeTag, E: c.WeakTypeTag, T: c.WeakTypeTag](c: blackbox.Context)(expr: c.Expr[E => T]): c.Expr[LTCol[LO, RO, T]] = {
    import c.universe._

    val leftType  = c.weakTypeOf[LO]
    val rightType = c.weakTypeOf[RO]
    val tType     = c.weakTypeOf[T]
    val name      = extractArgName[E, T](c)(expr)

    c.Expr(
      q"""new joinwiz.LTCol[$leftType, $rightType, $tType] {
            import org.apache.spark.sql.functions.col
            override def apply(value: $leftType): $tType = ($expr compose ${c.prefix}.orig)(value)
            override def column = col((${c.prefix}.names :+ $name).mkString("."))
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
            override def apply(value: $rightType): $tType = ($expr compose ${c.prefix}.orig)(value)
            override def column = col((${c.prefix}.names :+ $name).mkString("."))
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
