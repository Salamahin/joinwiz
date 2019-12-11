package joinwiz

import scala.annotation.tailrec
import scala.language.experimental.macros
import scala.reflect.macros.blackbox


case class LTColumn[S](name: String)

case class RTColumn[S](name: String)

class LTColumnExtractor[T] {
  def apply[S](expr: T => S): LTColumn[S] = macro TypedColumnNameExtractorMacro.leftColumn[T, S]
}

class RTColumnExtractor[T] {
  def apply[S](expr: T => S): RTColumn[S] = macro TypedColumnNameExtractorMacro.rightColumn[T, S]
}

private[fm] object TypedColumnNameExtractorMacro {
  def leftColumn[T: c.WeakTypeTag, S: c.WeakTypeTag]
  (c: blackbox.Context)
  (expr: c.Expr[T => S]): c.Expr[LTColumn[S]] = {
    import c.universe._

    val sType = c.weakTypeOf[S]
    val name = extractArgName[T, S](c)(expr)

    c.Expr(q"joinwiz.LTColumn[$sType]($name)")
  }

  def rightColumn[T: c.WeakTypeTag, S: c.WeakTypeTag]
  (c: blackbox.Context)
  (expr: c.Expr[T => S]): c.Expr[RTColumn[S]] = {
    import c.universe._

    val sType = c.weakTypeOf[S]
    val name = extractArgName[T, S](c)(expr)

    c.Expr(q"joinwiz.RTColumn[$sType]($name)")
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

