package joinwiz.spark

import joinwiz._
import joinwiz.expression.ExpressionEvaluator
import joinwiz.spark.SparkOperations.{LEFT_DS_ALIAS, RIGHT_DS_ALIAS}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object SparkExpressionEvaluator {
  implicit class LeftTypedColumnSyntax(ltc: LeftTypedColumn[_]) {
    def name                      = ltc.prefixes.mkString(".")
    def column(prefixes: String*) = col((prefixes ++ ltc.prefixes).mkString("."))
  }

  implicit class RightTypedColumnSyntax(rtc: RightTypedColumn[_]) {
    def name                      = rtc.prefixes.mkString(".")
    def column(prefixes: String*) = col((prefixes ++ rtc.prefixes).mkString("."))
  }
}

class SparkExpressionEvaluator extends ExpressionEvaluator[Any, Any, Column] {

  import SparkExpressionEvaluator._
  import org.apache.spark.sql.functions._

  private def constant(o: Const) = o match {
    case Const(Some(x)) => lit(x)
    case Const(None)    => lit(null)
    case Const(x)       => lit(x)
  }

  override protected def and(left: Column, right: Column): Column = left and right

  override protected def colEqCol(left: LeftTypedColumn[_], right: RightTypedColumn[_]): Column =
    left.column(LEFT_DS_ALIAS) === right.column(RIGHT_DS_ALIAS)

  override protected def leftColEqConst(left: LeftTypedColumn[_], const: Const): Column =
    left.column(LEFT_DS_ALIAS) === constant(const)

  override protected def rightColEqConst(right: RightTypedColumn[_], const: Const): Column =
    right.column(RIGHT_DS_ALIAS) === constant(const)

  override protected def colLessCol(left: LeftTypedColumn[_], right: RightTypedColumn[_]): Column =
    left.column(LEFT_DS_ALIAS) < right.column(RIGHT_DS_ALIAS)

  override protected def colGreatCol(left: LeftTypedColumn[_], right: RightTypedColumn[_]): Column =
    left.column(LEFT_DS_ALIAS) > right.column(RIGHT_DS_ALIAS)

  override protected def colLessOrEqCol(left: LeftTypedColumn[_], right: RightTypedColumn[_]): Column =
    left.column(LEFT_DS_ALIAS) <= right.column(RIGHT_DS_ALIAS)

  override protected def colGreatOrEqCol(left: LeftTypedColumn[_], right: RightTypedColumn[_]): Column =
    left.column(LEFT_DS_ALIAS) >= right.column(RIGHT_DS_ALIAS)

  override protected def leftColLessConst(left: LeftTypedColumn[_], const: Const): Column =
    left.column(LEFT_DS_ALIAS) < constant(const)

  override protected def leftColGreatConst(left: LeftTypedColumn[_], const: Const): Column =
    left.column(LEFT_DS_ALIAS) > constant(const)

  override protected def leftColLessOrEqConst(left: LeftTypedColumn[_], const: Const): Column =
    left.column(LEFT_DS_ALIAS) <= constant(const)

  override protected def leftCollGreatOrEqConst(left: LeftTypedColumn[_], const: Const): Column =
    left.column(LEFT_DS_ALIAS) >= constant(const)

  override protected def rightColLessConst(right: RightTypedColumn[_], const: Const): Column =
    right.column(RIGHT_DS_ALIAS) < constant(const)

  override protected def rightColGreatConst(right: RightTypedColumn[_], const: Const): Column =
    right.column(RIGHT_DS_ALIAS) > constant(const)

  override protected def rightColLessOrEqConst(right: RightTypedColumn[_], const: Const): Column =
    right.column(RIGHT_DS_ALIAS) <= constant(const)

  override protected def rightCollGreatOrEqConst(right: RightTypedColumn[_], const: Const): Column =
    right.column(RIGHT_DS_ALIAS) >= constant(const)
}
