package joinwiz.testkit

import joinwiz._

class SeqExpressionEvaluator[L, R](op: Expression) {
  def evaluate(left: L, right: R): Boolean = op match {
    case Equality(l: LeftTypedColumn[L, _, _], r: RightTypedColumn[R, _, _]) => l(left) == r(right)
    case Equality(l: LeftTypedColumn[L, _, _], r: Const[_])          => l(left) == r.value
    case Equality(l: Const[_], r: LeftTypedColumn[L, _, _])          => l.value == r(left)
    case Equality(l: RightTypedColumn[R, _, _], r: Const[_])          => l(right) == r.value
    case Equality(l: Const[_], r: RightTypedColumn[R, _, _])          => l.value == r(right)

    case And(l, r) =>
      new SeqExpressionEvaluator(l).evaluate(left, right) && new SeqExpressionEvaluator(r).evaluate(left, right)

    case x => throw new UnsupportedOperationException(s"Failed to evaluate expression $x")
  }
}
