package joinwiz

import org.apache.spark.sql.Column
import JoinWiz.{LEFT_DS_ALIAS, RIGHT_DS_ALIAS}

sealed trait Operand

case class LeftField(name: String) extends Operand

case class RightField(name: String) extends Operand

case class Const[T](value: T) extends Operand

sealed trait Operator

case class Equality(left: Operand, right: Operand) extends Operator

case class And(left: Operator, right: Operator) extends Operator

object Operator {

  import org.apache.spark.sql.functions._

  private def column(o: Operand) = o match {
    case LeftField(name) => col(s"$LEFT_DS_ALIAS.$name")
    case RightField(name) => col(s"$RIGHT_DS_ALIAS.$name")
    case Const(value) => lit(value)
  }

  def evaluate(e: Operator): Column = e match {
    case Equality(left, right) => column(left) === column(right)
    case And(left, right) => evaluate(left) and evaluate(right)
  }
}