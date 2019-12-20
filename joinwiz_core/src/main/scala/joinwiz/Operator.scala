package joinwiz

import org.apache.spark.sql.Column
import JoinWiz.{LEFT_DS_ALIAS, RIGHT_DS_ALIAS}

class ColumnEvaluator {

  import org.apache.spark.sql.functions._

  private def column(o: Operand) = o match {
    case LTColumn(name, _) => col(s"$LEFT_DS_ALIAS.$name")
    case RTColumn(name, _) => col(s"$RIGHT_DS_ALIAS.$name")
    case Const(value) => lit(value)
  }

  def evaluate(e: Operator): Column = e match {
    case Equality(left, right) => column(left) === column(right)
    case And(left, right) => evaluate(left) and evaluate(right)
  }
}