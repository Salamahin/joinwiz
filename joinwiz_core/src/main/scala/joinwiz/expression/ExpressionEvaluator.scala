package joinwiz.expression

import joinwiz._

trait ExpressionEvaluator[L, R, T] {

  protected def and(left: T, right: T): T
  protected def colEqCol(left: LeftTypedColumn[_], right: RightTypedColumn[_]): T
  protected def leftColEqConst(left: LeftTypedColumn[_], const: Const): T
  protected def rightColEqConst(right: RightTypedColumn[_], const: Const): T
  protected def colLessCol(left: LeftTypedColumn[_], right: RightTypedColumn[_]): T
  protected def colGreatCol(left: LeftTypedColumn[_], right: RightTypedColumn[_]): T
  protected def colLessOrEqCol(left: LeftTypedColumn[_], right: RightTypedColumn[_]): T
  protected def colGreatOrEqCol(left: LeftTypedColumn[_], right: RightTypedColumn[_]): T
  protected def leftColLessConst(left: LeftTypedColumn[_], const: Const): T
  protected def leftColGreatConst(left: LeftTypedColumn[_], const: Const): T
  protected def leftColLessOrEqConst(left: LeftTypedColumn[_], const: Const): T
  protected def leftCollGreatOrEqConst(left: LeftTypedColumn[_], const: Const): T
  protected def rightColLessConst(right: RightTypedColumn[_], const: Const): T
  protected def rightColGreatConst(right: RightTypedColumn[_], const: Const): T
  protected def rightColLessOrEqConst(right: RightTypedColumn[_], const: Const): T
  protected def rightCollGreatOrEqConst(right: RightTypedColumn[_], const: Const): T

  final def evaluate(e: Expression): T = e match {
    case Equality(left: LeftTypedColumn[_], right: RightTypedColumn[_]) => colEqCol(left, right)
    case Equality(right: RightTypedColumn[_], left: LeftTypedColumn[_]) => colEqCol(left, right)

    case Equality(left: LeftTypedColumn[_], const: Const)   => leftColEqConst(left, const)
    case Equality(right: RightTypedColumn[_], const: Const) => rightColEqConst(right, const)

    case Less(left: LeftTypedColumn[_], right: RightTypedColumn[_])        => colLessCol(left, right)
    case Greater(left: LeftTypedColumn[_], right: RightTypedColumn[_])     => colGreatCol(left, right)
    case LessOrEq(left: LeftTypedColumn[_], right: RightTypedColumn[_])    => colLessOrEqCol(left, right)
    case GreaterOrEq(left: LeftTypedColumn[_], right: RightTypedColumn[_]) => colGreatOrEqCol(left, right)

    case Less(left: LeftTypedColumn[_], const: Const)        => leftColLessConst(left, const)
    case Greater(left: LeftTypedColumn[_], const: Const)     => leftColGreatConst(left, const)
    case LessOrEq(left: LeftTypedColumn[_], const: Const)    => leftColLessOrEqConst(left, const)
    case GreaterOrEq(left: LeftTypedColumn[_], const: Const) => leftCollGreatOrEqConst(left, const)

    case Less(right: RightTypedColumn[_], const: Const)        => rightColLessConst(right, const)
    case Greater(right: RightTypedColumn[_], const: Const)     => rightColGreatConst(right, const)
    case LessOrEq(right: RightTypedColumn[_], const: Const)    => rightColLessOrEqConst(right, const)
    case GreaterOrEq(right: RightTypedColumn[_], const: Const) => rightCollGreatOrEqConst(right, const)

    case And(left, right) => and(evaluate(left), evaluate(right))

    case x => throw new UnsupportedOperationException(s"Failed to evaluate expression $x")
  }
}
