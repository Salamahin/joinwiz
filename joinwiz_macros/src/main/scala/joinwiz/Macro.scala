package joinwiz

import scala.annotation.tailrec
import scala.language.experimental.macros
import scala.reflect.macros.blackbox


trait LTColumn[S] {
  val name: String
  type Orig
  val expr: Orig => S
}

trait RTColumn[S] {
  val name: String
  type Orig
  val expr: Orig => S
}

class LTColumnExtractor[T] {
  def apply[S](expr: T => S): LTColumn[S] = macro TypedColumnNameExtractorMacro.leftColumn[T, S]
}

class RTColumnExtractor[T] {
  def apply[S](expr: T => S): RTColumn[S] = macro TypedColumnNameExtractorMacro.rightColumn[T, S]
}

private object TypedColumnNameExtractorMacro {
  def leftColumn[T: c.WeakTypeTag, S: c.WeakTypeTag]
  (c: blackbox.Context)
  (expr: c.Expr[T => S]): c.Expr[LTColumn[S]] = {
    import c.universe._

    val sType = c.weakTypeOf[S]
    val tType = c.weakTypeOf[T]
    val name = extractArgName[T, S](c)(expr)

    c.Expr(
      q"""new joinwiz.LTColumn[$sType] {
            override val name = $name
            override type Orig = $tType
            override val expr: $tType => $sType = $expr
      }""")
  }

  def rightColumn[T: c.WeakTypeTag, S: c.WeakTypeTag]
  (c: blackbox.Context)
  (expr: c.Expr[T => S]): c.Expr[RTColumn[S]] = {
    import c.universe._

    val sType = c.weakTypeOf[S]
    val tType = c.weakTypeOf[T]
    val name = extractArgName[T, S](c)(expr)

    c.Expr(
      q"""new joinwiz.RTColumn[$sType] {
            override val name = $name
            override type Orig = $tType
            override val expr: $tType => $sType = $expr
      }""")
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

