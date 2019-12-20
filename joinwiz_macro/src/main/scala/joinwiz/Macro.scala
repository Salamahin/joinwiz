package joinwiz

import scala.annotation.tailrec
import scala.language.experimental.macros
import scala.reflect.macros.blackbox

case class Equality(left: Operand, right: Operand) extends Operator

case class And(left: Operator, right: Operator) extends Operator

sealed trait Operand

case class Const[T](value: T) extends Operand

sealed trait Operator

case class LTColumn[T, S](name: String, expr: T => S) extends Operand

case class  RTColumn[T, S](name: String, expr: T => S) extends Operand

class LTColumnExtractor[T] {
  def apply[S](expr: T => S): LTColumn[T, S] = macro TypedColumnNameExtractorMacro.leftColumn[T, S]
}

class RTColumnExtractor[T] {
  def apply[S](expr: T => S): RTColumn[T, S] = macro TypedColumnNameExtractorMacro.rightColumn[T, S]
}

private object TypedColumnNameExtractorMacro {
  def leftColumn[T: c.WeakTypeTag, S: c.WeakTypeTag]
  (c: blackbox.Context)
  (expr: c.Expr[T => S]): c.Expr[LTColumn[T, S]] = {
    import c.universe._

    val sType = c.weakTypeOf[S]
    val tType = c.weakTypeOf[T]
    val name = extractArgName[T, S](c)(expr)

    c.Expr(q"joinwiz.LTColumn[$tType, $sType]($name, $expr)")
  }

  def rightColumn[T: c.WeakTypeTag, S: c.WeakTypeTag]
  (c: blackbox.Context)
  (expr: c.Expr[T => S]): c.Expr[RTColumn[T, S]] = {
    import c.universe._

    val sType = c.weakTypeOf[S]
    val tType = c.weakTypeOf[T]
    val name = extractArgName[T, S](c)(expr)

    c.Expr(q"joinwiz.RTColumn[$tType, $sType]($name, $expr)")
  }

  private def extractArgName[T: c.WeakTypeTag, S: c.WeakTypeTag]
  (c: blackbox.Context)
  (expr: c.Expr[T => S]): String = {
    import c.universe._

    @tailrec
    def extract(tree: c.Tree, acc: List[String]): List[String] = {
      tree match {
        case Ident(_) => acc
        case Select(q, n) => extract(q, n.decodedName.toString :: acc)
        case Function(_, body) => extract(body, acc)
        case _ => c.abort(c.enclosingPosition, s"Unsupported expression: $expr")
      }
    }

    extract(expr.tree, Nil).mkString(".")
  }
}

