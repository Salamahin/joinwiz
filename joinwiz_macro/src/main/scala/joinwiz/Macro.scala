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

sealed trait TCol[K, +T] extends Value {
  def apply(value: K): T
}

abstract class LTCol[K, +T: TypeTag] extends TCol[K, T] {
  self =>

  def map[S: TypeTag](f: T => S) = new LTCol[K, S] {
    override def column: Column     = udf((x: T) => f(x)) apply self.column
    override def apply(value: K): S = (f compose self.apply)(value)
  }
}

abstract class RTCol[K, +T: TypeTag] extends TCol[K, T] {
  self =>

  def map[S: TypeTag](f: T => S) = new RTCol[K, S] {
    override def column: Column     = udf((x: T) => f(x)) apply self.column
    override def apply(value: K): S = (f compose self.apply)(value)
  }
}

sealed trait Expression
final case class And(left: Expression, right: Expression) extends Expression
final case class Equality(left: Value, right: Value)      extends Expression
final case class Less(left: Value, right: Value)          extends Expression
final case class Greater(left: Value, right: Value)       extends Expression
final case class LessOrEq(left: Value, right: Value)      extends Expression
final case class GreaterOrEq(left: Value, right: Value)   extends Expression

class ApplyLeft[E](val names: Seq[String]) extends Serializable {
  def apply[T](expr: E => T): LTCol[E, T] = macro ApplyCol.leftColumn[E, T]
}

class ApplyRight[E](val names: Seq[String]) extends Serializable {
  def apply[T](expr: E => T): RTCol[E, T] = macro ApplyCol.rightColumn[E, T]
}

object ApplyLeft {
  def apply[E] = new ApplyLeft[E](names = Left.alias :: Nil)
}

object ApplyRight {
  def apply[E] = new ApplyRight[E](names = Right.alias :: Nil)
}

object Left {
  val alias = "LEFT"
}
object Right {
  val alias = "RIGHT"
}

private object ApplyCol {
  def leftColumn[E: c.WeakTypeTag, T: c.WeakTypeTag](
    c: blackbox.Context
  )(expr: c.Expr[E => T]): c.Expr[LTCol[E, T]] = {
    import c.universe._

    val eType = c.weakTypeOf[E]
    val tType = c.weakTypeOf[T]
    val name  = extractArgName[E, T](c)(expr)

    c.Expr(
      q"""new joinwiz.LTCol[$eType, $tType] {
            import org.apache.spark.sql.functions.col
            override def apply(value: $eType): $tType = $expr(value)
            override def column = col((${c.prefix}.names :+ $name).mkString("."))
          }"""
    )
  }

  def rightColumn[E: c.WeakTypeTag, T: c.WeakTypeTag](
    c: blackbox.Context
  )(expr: c.Expr[E => T]): c.Expr[RTCol[E, T]] = {
    import c.universe._

    val eType = c.weakTypeOf[E]
    val tType = c.weakTypeOf[T]
    val name  = extractArgName[E, T](c)(expr)

    c.Expr(
      q"""new joinwiz.RTCol[$eType, $tType] {
            import org.apache.spark.sql.functions.col
            override def apply(value: $eType): $tType = $expr(value)
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
