package joinwiz.spark

import joinwiz._
import joinwiz.expression.ExpressionEvaluator
import joinwiz.spark.SparkOperations.{LEFT_DS_ALIAS, RIGHT_DS_ALIAS}
import org.apache.spark.sql.Column

class SparkExpressionEvaluator extends ExpressionEvaluator[Any, Any, Column] {

  import org.apache.spark.sql.functions._

  private def column(o: TypedCol) = o match {
    case LeftTypedColumn(name)  => col(s"$LEFT_DS_ALIAS.$name")
    case RightTypedColumn(name) => col(s"$RIGHT_DS_ALIAS.$name")
  }

  private def constant(o: Const) = o match {
    case Const(x) => lit(x)
  }

  override protected def and(left: Column, right: Column): Column = left and right

  override protected def colEqCol(left: LeftTypedColumn[Any, _, _], right: RightTypedColumn[Any, _, _]): Column =
    column(left) === column(right)

  override protected def leftColEqConst(left: LeftTypedColumn[Any, _, _], const: Const): Column =
    column(left) === constant(const)

  override protected def rightColEqConst(right: RightTypedColumn[Any, _, _], const: Const): Column =
    column(right) === constant(const)

  override protected def colLessCol(left: LeftTypedColumn[Any, _, _], right: RightTypedColumn[Any, _, _]): Column =
    column(left) < column(right)

  override protected def colGreatCol(left: LeftTypedColumn[Any, _, _], right: RightTypedColumn[Any, _, _]): Column =
    column(left) > column(right)

  override protected def colLessOrEqCol(left: LeftTypedColumn[Any, _, _], right: RightTypedColumn[Any, _, _]): Column =
    column(left) <= column(right)

  override protected def colGreatOrEqCol(left: LeftTypedColumn[Any, _, _], right: RightTypedColumn[Any, _, _]): Column =
    column(left) >= column(right)

  override protected def leftColLessConst(left: LeftTypedColumn[Any, _, _], const: Const): Column =
    column(left) < constant(const)

  override protected def leftColGreatConst(left: LeftTypedColumn[Any, _, _], const: Const): Column =
    column(left) > constant(const)

  override protected def leftColLessOrEqConst(left: LeftTypedColumn[Any, _, _], const: Const): Column =
    column(left) <= constant(const)

  override protected def leftCollGreatOrEqConst(left: LeftTypedColumn[Any, _, _], const: Const): Column =
    column(left) >= constant(const)

  override protected def rightColLessConst(right: RightTypedColumn[Any, _, _], const: Const): Column =
    column(right) < constant(const)

  override protected def rightColGreatConst(right: RightTypedColumn[Any, _, _], const: Const): Column =
    column(right) > constant(const)

  override protected def rightColLessOrEqConst(right: RightTypedColumn[Any, _, _], const: Const): Column =
    column(right) <= constant(const)

  override protected def rightCollGreatOrEqConst(right: RightTypedColumn[Any, _, _], const: Const): Column =
    column(right) >= constant(const)
}
