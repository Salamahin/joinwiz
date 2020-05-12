package joinwiz

import scala.annotation.tailrec
import scala.language.experimental.macros
import scala.language.higherKinds
import scala.reflect.macros.blackbox

sealed trait Value
sealed trait Expression

sealed trait TypedCol              extends Value
final case class Const(value: Any) extends Value

final case class And(left: Expression, right: Expression) extends Expression
final case class Equality(left: Value, right: Value)      extends Expression
final case class Less(left: Value, right: Value)          extends Expression
final case class Greater(left: Value, right: Value)       extends Expression
final case class LessOrEq(left: Value, right: Value)      extends Expression
final case class GreaterOrEq(left: Value, right: Value)   extends Expression

final case class LeftTypedColumn[T](prefixes: Seq[String])  extends TypedCol
final case class RightTypedColumn[T](prefixes: Seq[String]) extends TypedCol

class ApplyToLeftColumn[E](val prefixes: Seq[String]) {
  def apply[T](expr: E => T): LeftTypedColumn[T] = macro ApplyToColumn.leftColumn[E, T]
}

class ApplyToRightColumn[E](val prefixes: Seq[String]) {
  def apply[T](expr: E => T): RightTypedColumn[T] = macro ApplyToColumn.rightColumn[E, T]
}

object ApplyToLeftColumn {
  def apply[E] = new ApplyToLeftColumn[E](prefixes = Nil)
}

object ApplyToRightColumn {
  def apply[E] = new ApplyToRightColumn[E](prefixes = Nil)
}

private object ApplyToColumn {
  def leftColumn[E: c.WeakTypeTag, T: c.WeakTypeTag](
    c: blackbox.Context
  )(expr: c.Expr[E => T]): c.Expr[LeftTypedColumn[T]] = {
    import c.universe._

    val tType = c.weakTypeOf[T]
    val name  = extractArgName[E, T](c)(expr)

    c.Expr(q"joinwiz.LeftTypedColumn[$tType](${c.prefix}.prefixes :+ $name)")
  }

  def rightColumn[E: c.WeakTypeTag, T: c.WeakTypeTag](
    c: blackbox.Context
  )(expr: c.Expr[E => T]): c.Expr[RightTypedColumn[T]] = {
    import c.universe._

    val tType = c.weakTypeOf[T]
    val name  = extractArgName[E, T](c)(expr)

    c.Expr(q"joinwiz.RightTypedColumn[$tType](${c.prefix}.prefixes :+ $name)")
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
}
