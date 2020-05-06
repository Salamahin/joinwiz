package joinwiz.spark

import joinwiz._
import joinwiz.spark.SparkOperations.{LEFT_DS_ALIAS, RIGHT_DS_ALIAS}
import org.apache.spark.sql.Column

class SparkExpressionEvaluator {

  import org.apache.spark.sql.functions._

  def evaluate(e: Expression): Column = e match {
    case Equality(left: TypedCol, right: TypedCol) => column(left) === column(right)
    case Equality(left: TypedCol, right: Const)    => column(left) === const(right)
    case Equality(left: Const, right: TypedCol)    => column(right) === const(left)

    case And(left, right)                          => evaluate(left) and evaluate(right)

    case x => throw new UnsupportedOperationException(s"Failed to evaluate expression $x")
  }

  private def column(o: TypedCol) = o match {
    case LeftTypedColumn(name)  => col(s"$LEFT_DS_ALIAS.$name")
    case RightTypedColumn(name) => col(s"$RIGHT_DS_ALIAS.$name")
  }

  private def const(o: Const) = o match {
    case Const(Some(x)) => lit(x)
    case Const(None)    => lit(null)
    case Const(x)       => lit(x)
  }
}
