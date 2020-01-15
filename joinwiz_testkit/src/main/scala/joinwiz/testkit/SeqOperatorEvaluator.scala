package joinwiz.testkit

import joinwiz._

class SeqOperatorEvaluator[L, R](op: Operator) {
  def evaluate(left: L, right: R): Boolean = op match {
    case Equality(l: LTColumn[L, _, _], r: RTColumn[R, _, _]) => l(left) == r(right)

    case Equality(l: LTColumn[L, _, _], r: Const[_]) => l(left) == r.value
    case Equality(l: Const[_], r: LTColumn[L, _, _]) => l.value == r(left)

    case Equality(l: RTColumn[R, _, _], r: Const[_]) => l(right) == r.value
    case Equality(l: Const[_], r: RTColumn[R, _, _]) => l.value == r(right)

    case And(l, r) =>
      new SeqOperatorEvaluator(l).evaluate(left, right) && new SeqOperatorEvaluator(r).evaluate(left, right)

    case x => throw new UnsupportedOperationException(s"Failed to evaluate expression $x")
  }
}
