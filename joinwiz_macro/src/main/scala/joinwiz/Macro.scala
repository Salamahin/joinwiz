package joinwiz

import scala.annotation.tailrec
import scala.language.experimental.macros
import scala.reflect.macros.blackbox

case class Equality(left: Operand, right: Operand) extends Operator {
  override def toString: String = s"$left =:= $right"
}

case class And(left: Operator, right: Operator) extends Operator {
  override def toString: String = s"$left && $right"
}

sealed trait Operand

case class Const[T](value: T) extends Operand {
  override def toString: String = s"const($value)"
}

sealed trait Operator

sealed trait TColumn

trait LTColumn[O, E, T] extends Operand with TColumn {
  def apply(source: O): T

  val name: String

  override def toString: String = s"left($name)"
}

object LTColumn {
  def unapply(c: LTColumn[_, _, _]) = Some(c.name)
}

case class RTColumn[T, S](name: String, expr: T => S) extends Operand with TColumn {
  override def toString: String = s"right($name)"
}

class LTColumnExtractor[O, E](val prefix: Seq[String] = Nil, val extractor: O => E) {
  def apply[T](expr: E => T): LTColumn[O, E, T] = macro TypedColumnNameExtractorMacro.leftColumn[O, E, T]
}

object LTColumnExtractor {
  def apply[T] = new LTColumnExtractor[T, T](extractor = identity)
}

class RTColumnExtractor[T](val level: Int = 0) {
  def apply[S](expr: T => S): RTColumn[T, S] = macro TypedColumnNameExtractorMacro.rightColumn[T, S]
}

private object TypedColumnNameExtractorMacro {
  def leftColumn[O: c.WeakTypeTag, E: c.WeakTypeTag, T: c.WeakTypeTag]
  (c: blackbox.Context)
  (expr: c.Expr[E => T]): c.Expr[LTColumn[O, E, T]] = {
    import c.universe._

    val tType = c.weakTypeOf[T]
    val eType = c.weakTypeOf[E]
    val oType = c.weakTypeOf[O]

    val name = extractArgName[E, T](c)(expr)

    c.Expr(
      q"""new joinwiz.LTColumn[$oType, $eType, $tType] {
            override def apply(source: $oType): $tType = $expr(${c.prefix}.extractor(source))

            override val name: String = (${c.prefix}.prefix :+ $name).mkString(".")
          }""")
  }

  private def extractArgName[E: c.WeakTypeTag, T: c.WeakTypeTag]
  (c: blackbox.Context)
  (func: c.Expr[E => T]): String = {
    import c.universe._

    @tailrec
    def extract(tree: c.Tree, acc: List[String]): List[String] = {
      tree match {
        case Ident(_) => acc
        case Select(q, n) => extract(q, n.decodedName.toString :: acc)
        case Function(_, body) => extract(body, acc)
        case _ => c.abort(c.enclosingPosition, s"Unsupported expression: $func")
      }
    }

    extract(func.tree, Nil).mkString(".")
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
}

