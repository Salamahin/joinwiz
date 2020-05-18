package joinwiz.spark

import joinwiz._
import joinwiz.expression.ExpressionEvaluator
import org.apache.spark.sql.Column

object SparkExpressionEvaluator extends ExpressionEvaluator[Column] {
  override protected def and(left: Column, right: Column): Column = left && right

  override protected def equal(left: Value, right: Value): Column = left.column === right.column

  override protected def less(left: Value, right: Value): Column    = left.column < right.column
  override protected def greater(left: Value, right: Value): Column = left.column > right.column

  override protected def lessOrEqual(left: Value, right: Value): Column    = left.column <= right.column
  override protected def greaterOrEqual(left: Value, right: Value): Column = left.column >= right.column
}
