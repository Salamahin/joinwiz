package joinwiz

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

import scala.annotation.tailrec
import scala.language.experimental.macros
import scala.language.higherKinds
import scala.reflect.macros.blackbox
import scala.reflect.runtime.universe.TypeTag

sealed trait Value extends Serializable {
  def column: Column
}

final case class Const[T](value: T) extends Value {
  override def column: Column = lit(value)
}

sealed trait TCol[O, +T] extends Value {
  type Orig = O
  def apply(value: O): T
}

abstract class LTCol[O, T: TypeTag] extends TCol[O, T]
abstract class RTCol[O, +T: TypeTag] extends TCol[O, T]

sealed trait Expression
final case class And(left: Expression, right: Expression) extends Expression
final case class Equality(left: Value, right: Value)      extends Expression
final case class Less(left: Value, right: Value)          extends Expression
final case class Greater(left: Value, right: Value)       extends Expression
final case class LessOrEq(left: Value, right: Value)      extends Expression
final case class GreaterOrEq(left: Value, right: Value)   extends Expression

sealed class ApplyLeft[O, E](val names: Seq[String], val orig: O => E) extends Serializable {
  def apply[T](expr: E => T): LTCol[O, T] = macro ApplyCol.leftColumn[O, E, T]

  private[joinwiz] def map[E1](name: String, newOrig: E => E1) =
    new ApplyLeft[O, E1](names :+ name, newOrig compose orig)
}

sealed class ApplyRight[O, E](val names: Seq[String], val orig: O => E) extends Serializable {
  def apply[T](expr: E => T): RTCol[O, T] = macro ApplyCol.rightColumn[O, E, T]

  private[joinwiz] def map[E1](name: String, newOrig: E => E1) =
    new ApplyRight[O, E1](names :+ name, newOrig compose orig)
}

object ApplyLeft {
  def apply[E] = new ApplyLeft[E, E](names = Left.alias :: Nil, identity)
}

object ApplyRight {
  def apply[E] = new ApplyRight[E, E](names = Right.alias :: Nil, identity)
}

private[joinwiz] object Left {
  val alias = "LEFT"
}
private[joinwiz] object Right {
  val alias = "RIGHT"
}

private object ApplyCol {
  def leftColumn[O: c.WeakTypeTag, E: c.WeakTypeTag, T: c.WeakTypeTag](
    c: blackbox.Context
  )(expr: c.Expr[E => T]): c.Expr[LTCol[O, T]] = {
    import c.universe._

    val oType = c.weakTypeOf[O]
    val tType = c.weakTypeOf[T]
    val name  = extractArgName[E, T](c)(expr)

    c.Expr(
      q"""new joinwiz.LTCol[$oType, $tType] {
            import org.apache.spark.sql.functions.col
            override def apply(value: $oType): $tType = ($expr compose ${c.prefix}.orig)(value)
            override def column = col((${c.prefix}.names :+ $name).mkString("."))
          }"""
    )
  }

  def rightColumn[O: c.WeakTypeTag, E: c.WeakTypeTag, T: c.WeakTypeTag](
    c: blackbox.Context
  )(expr: c.Expr[E => T]): c.Expr[RTCol[O, T]] = {
    import c.universe._

    val oType = c.weakTypeOf[O]
    val tType = c.weakTypeOf[T]
    val name  = extractArgName[E, T](c)(expr)

    c.Expr(
      q"""new joinwiz.RTCol[$oType, $tType] {
            import org.apache.spark.sql.functions.col
            override def apply(value: $oType): $tType = ($expr compose ${c.prefix}.orig)(value)
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
        case _                 => c.abort(c.enclosingPosition, s"Unsupported expression: $func")
      }
    }

    extract(func.tree, Nil).mkString(".")
  }
}
