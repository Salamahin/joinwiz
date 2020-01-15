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

/**
 * Left typed column. Contains info about column name and extractor
 * When joining F[L] and F[R], to unsure type safety several types are required.
 * First of all we need to know the original type L to extract value from F container.
 * Second, if L is a tuple of (A, b), we can de-compose extractor into 2 extractors - for A and for B respectively
 * Third, we need to know the column type itself to prevent joining by incompatible types
 *
 * @tparam O original type of left operand in join
 * @tparam E decomposed type, is used when unapplying original extractor
 * @tparam T column type
 */
trait LTColumn[O, E, T] extends Operand with TColumn {
  def apply(source: O): T

  val name: String

  override def toString: String = s"left($name)"
}

object LTColumn {
  def unapply(c: LTColumn[_, _, _]): Option[String] = Some(c.name)
}

object RTColumn {
  def unapply(c: RTColumn[_, _, _]): Option[String] = Some(c.name)
}

/**
 * Right typed column. Contains info about column name and extractor
 * When joining F[L] and F[R], to unsure type safety several types are required.
 * First of all we need to know the original type R to extract value from F container.
 * Second, if R is a tuple of (A, b), we can de-compose extractor into 2 extractors - for A and for B respectively
 * Third, we need to know the column type itself to prevent joining by incompatible types
 *
 * @tparam O original type of right operand in join
 * @tparam E decomposed type, is used when unapplying original extractor
 * @tparam T column type
 */
trait RTColumn[O, E, T] extends Operand with TColumn {
  def apply(source: O): T

  val name: String

  override def toString: String = s"right($name)"
}

class LTColumnExtractor[O, E](val prefixes: Seq[String], val extractor: O => E) {
  def apply[T](expr: E => T): LTColumn[O, E, T] = macro TypedColumnNameExtractorMacro.leftColumn[O, E, T]
}

class RTColumnExtractor[O, E](val prefixes: Seq[String], val extractor: O => E) {
  def apply[T](expr: E => T): RTColumn[O, E, T] = macro TypedColumnNameExtractorMacro.rightColumn[O, E, T]
}

object LTColumnExtractor {
  def apply[T] = new LTColumnExtractor[T, T](prefixes = Nil, extractor = identity)
}

object RTColumnExtractor {
  def apply[T] = new RTColumnExtractor[T, T](prefixes = Nil, extractor = identity)
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

            override val name: String = (${c.prefix}.prefixes :+ $name).mkString(".")
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

  def rightColumn[O: c.WeakTypeTag, E: c.WeakTypeTag, T: c.WeakTypeTag]
  (c: blackbox.Context)
  (expr: c.Expr[E => T]): c.Expr[RTColumn[O, E, T]] = {
    import c.universe._

    val tType = c.weakTypeOf[T]
    val eType = c.weakTypeOf[E]
    val oType = c.weakTypeOf[O]

    val name = extractArgName[E, T](c)(expr)

    c.Expr(
      q"""new joinwiz.RTColumn[$oType, $eType, $tType] {
            override def apply(source: $oType): $tType = $expr(${c.prefix}.extractor(source))

            override val name: String = (${c.prefix}.prefixes :+ $name).mkString(".")
          }""")
  }
}

