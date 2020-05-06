package joinwiz

import scala.annotation.tailrec
import scala.language.experimental.macros
import scala.reflect.macros.blackbox

sealed trait Value
sealed trait Expression
sealed trait TypedCol extends Value

case class Const(value: Any) extends Value {
  override def toString: String = s"const($value)"
}

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
trait LeftTypedColumn[O, E, T] extends TypedCol {
  def apply(source: O): T

  val name: String

  override def toString: String = s"left($name)"
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
trait RightTypedColumn[O, E, T] extends TypedCol {
  def apply(source: O): T

  val name: String

  override def toString: String = s"right($name)"
}

case class And(left: Expression, right: Expression) extends Expression {
  override def toString: String = s"$left && $right"
}

case class Equality(left: Value, right: Value) extends Expression {
  override def toString: String = s"$left =:= $right"
}

case class Less(left: Value, right: Value) extends Expression {
  override def toString: String = s"$left < $right"
}

case class Greater(left: Value, right: Value) extends Expression {
  override def toString: String = s"$left > $right"
}

case class LessOrEq(left: Value, right: Value) extends Expression {
  override def toString: String = s"$left <= $right"
}

case class GreaterOrEq(left: Value, right: Value) extends Expression {
  override def toString: String = s"$left >= $right"
}

object LeftTypedColumn {
  def unapply(c: LeftTypedColumn[_, _, _]): Option[String] = Some(c.name)
}

object RightTypedColumn {
  def unapply(c: RightTypedColumn[_, _, _]): Option[String] = Some(c.name)
}

class ApplyToLeftColumn[O, E](val prefixes: Seq[String], val extractor: O => E) {
  def apply[T](expr: E => T): LeftTypedColumn[O, E, T] = macro ApplyToColumn.leftColumn[O, E, T]
}

class ApplyToRightColumn[O, E](val prefixes: Seq[String], val extractor: O => E) {
  def apply[T](expr: E => T): RightTypedColumn[O, E, T] = macro ApplyToColumn.rightColumn[O, E, T]
}

object ApplyToLeftColumn {
  def apply[T] = new ApplyToLeftColumn[T, T](prefixes = Nil, extractor = identity)
}

object ApplyToRightColumn {
  def apply[T] = new ApplyToRightColumn[T, T](prefixes = Nil, extractor = identity)
}

private object ApplyToColumn {
  def leftColumn[O: c.WeakTypeTag, E: c.WeakTypeTag, T: c.WeakTypeTag](
    c: blackbox.Context
  )(expr: c.Expr[E => T]): c.Expr[LeftTypedColumn[O, E, T]] = {
    import c.universe._

    val tType = c.weakTypeOf[T]
    val eType = c.weakTypeOf[E]
    val oType = c.weakTypeOf[O]

    val name = extractArgName[E, T](c)(expr)

    c.Expr(q"""new joinwiz.LeftTypedColumn[$oType, $eType, $tType] {
            override def apply(source: $oType): $tType = $expr(${c.prefix}.extractor(source))

            override val name: String = (${c.prefix}.prefixes :+ $name).mkString(".")
          }""")
  }

  private def extractArgName[E: c.WeakTypeTag, T: c.WeakTypeTag](c: blackbox.Context)(func: c.Expr[E => T]): String = {
    import c.universe._

    @tailrec
    def extract(tree: c.Tree, acc: List[String]): List[String] = {
      tree match {
        case Ident(_)          => acc
        case Select(q, n)      => extract(q, n.decodedName.toString :: acc)
        case Function(_, body) => extract(body, acc)
        case _                 => c.abort(c.enclosingPosition, s"Unsupported expression: $func")
      }
    }

    extract(func.tree, Nil).mkString(".")
  }

  def rightColumn[O: c.WeakTypeTag, E: c.WeakTypeTag, T: c.WeakTypeTag](
    c: blackbox.Context
  )(expr: c.Expr[E => T]): c.Expr[RightTypedColumn[O, E, T]] = {
    import c.universe._

    val tType = c.weakTypeOf[T]
    val eType = c.weakTypeOf[E]
    val oType = c.weakTypeOf[O]

    val name = extractArgName[E, T](c)(expr)

    c.Expr(q"""new joinwiz.RightTypedColumn[$oType, $eType, $tType] {
            override def apply(source: $oType): $tType = $expr(${c.prefix}.extractor(source))

            override val name: String = (${c.prefix}.prefixes :+ $name).mkString(".")
          }""")
  }
}
