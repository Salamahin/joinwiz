import scala.annotation.tailrec
import scala.reflect.macros.whitebox

package object joinwiz {
  type Id[T] = T

  object alias {
    val left  = "LEFT"
    val right = "RIGHT"
  }

  def argName[E: c.WeakTypeTag, T: c.WeakTypeTag](c: whitebox.Context)(func: c.Expr[E => T]): String = {
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
