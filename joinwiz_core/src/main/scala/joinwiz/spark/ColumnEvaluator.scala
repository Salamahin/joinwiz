package joinwiz.spark

import joinwiz._
import joinwiz.spark.SparkOperations.{LEFT_DS_ALIAS, RIGHT_DS_ALIAS}
import org.apache.spark.sql.Column

class ColumnEvaluator {

  import org.apache.spark.sql.functions._

  def evaluate(e: Expression): Column = e match {
    case Equality(left: TypedCol, right: TypedCol) => column(left) === column(right)
    case Equality(left: TypedCol, right: Const[_]) => column(left) === const(right)
    case Equality(left: Const[_], right: TypedCol) => column(right) === const(left)
    case And(left, right)                          => evaluate(left) and evaluate(right)

    case x => throw new UnsupportedOperationException(s"Failed to evaluate expression $x")
  }

  private def column(o: TypedCol) = o match {
    case LTColumn(name) => col(s"$LEFT_DS_ALIAS.$name")
    case RTColumn(name) => col(s"$RIGHT_DS_ALIAS.$name")
  }

  private def const(o: Const[_]) = o match {
    case Const(Some(x)) => lit(x)
    case Const(None)    => lit(null)
    case Const(x)       => lit(x)
  }
}
