package joinwiz.expression

import joinwiz._

trait ExpressionEvaluator[L, R, T] {

  protected def and(left: T, right: T): T
  protected def colEqCol(left: LeftTypedColumn[L, _, _], right: RightTypedColumn[R, _, _]): T
  protected def leftColEqConst(left: LeftTypedColumn[L, _, _], const: Const): T
  protected def rightColEqConst(right: RightTypedColumn[R, _, _], const: Const): T
  protected def colLessCol(left: LeftTypedColumn[L, _, _], right: RightTypedColumn[R, _, _]): T
  protected def colGreatCol(left: LeftTypedColumn[L, _, _], right: RightTypedColumn[R, _, _]): T
  protected def colLessOrEqCol(left: LeftTypedColumn[L, _, _], right: RightTypedColumn[R, _, _]): T
  protected def colGreatOrEqCol(left: LeftTypedColumn[L, _, _], right: RightTypedColumn[R, _, _]): T
  protected def leftColLessConst(left: LeftTypedColumn[L, _, _], const: Const): T
  protected def leftColGreatConst(left: LeftTypedColumn[L, _, _], const: Const): T
  protected def leftColLessOrEqConst(left: LeftTypedColumn[L, _, _], const: Const): T
  protected def leftCollGreatOrEqConst(left: LeftTypedColumn[L, _, _], const: Const): T
  protected def rightColLessConst(right: RightTypedColumn[R, _, _], const: Const): T
  protected def rightColGreatConst(right: RightTypedColumn[R, _, _], const: Const): T
  protected def rightColLessOrEqConst(right: RightTypedColumn[R, _, _], const: Const): T
  protected def rightCollGreatOrEqConst(right: RightTypedColumn[R, _, _], const: Const): T

  final def evaluate(e: Expression): T = e match {
    case Equality(left: LeftTypedColumn[L, _, _], right: RightTypedColumn[R, _, _]) => colEqCol(left, right)
    case Equality(right: RightTypedColumn[R, _, _], left: LeftTypedColumn[L, _, _]) => colEqCol(left, right)

    case Equality(left: LeftTypedColumn[L, _, _], const: Const)                     => leftColEqConst(left, const)
    case Equality(right: RightTypedColumn[R, _, _], const: Const)                   => rightColEqConst(right, const)

    case Less(left: LeftTypedColumn[L, _, _], right: RightTypedColumn[R, _, _])        => colLessCol(left, right)
    case Greater(left: LeftTypedColumn[L, _, _], right: RightTypedColumn[R, _, _])     => colGreatCol(left, right)
    case LessOrEq(left: LeftTypedColumn[L, _, _], right: RightTypedColumn[R, _, _])    => colLessOrEqCol(left, right)
    case GreaterOrEq(left: LeftTypedColumn[L, _, _], right: RightTypedColumn[R, _, _]) => colGreatOrEqCol(left, right)

    case Less(left: LeftTypedColumn[L, _, _], const: Const)        => leftColLessConst(left, const)
    case Greater(left: LeftTypedColumn[L, _, _], const: Const)     => leftColGreatConst(left, const)
    case LessOrEq(left: LeftTypedColumn[L, _, _], const: Const)    => leftColLessOrEqConst(left, const)
    case GreaterOrEq(left: LeftTypedColumn[L, _, _], const: Const) => leftCollGreatOrEqConst(left, const)

    case Less(right: RightTypedColumn[R, _, _], const: Const)        => rightColLessConst(right, const)
    case Greater(right: RightTypedColumn[R, _, _], const: Const)     => rightColGreatConst(right, const)
    case LessOrEq(right: RightTypedColumn[R, _, _], const: Const)    => rightColLessOrEqConst(right, const)
    case GreaterOrEq(right: RightTypedColumn[R, _, _], const: Const) => rightCollGreatOrEqConst(right, const)

    case And(left, right) => and(evaluate(left), evaluate(right))

    case x => throw new UnsupportedOperationException(s"Failed to evaluate expression $x")
  }
}
