package joinwiz.testkit.sparkless

import joinwiz._

class SeqOperatorEvaluator[T, U](op: Operator) {
  def evaluate(left: T, right: U): Boolean = op match {
    case Equality(l: LTColumn[T, _], r: RTColumn[U, _]) => l.expr(left) == r.expr(right)

    case Equality(l: LTColumn[T, _], r: Const[_]) => l.expr(left) == r.value
    case Equality(l: Const[_], r: LTColumn[T, _]) => l.value == r.expr(left)

    case Equality(l: RTColumn[U, _], r: Const[_]) => l.expr(right) == r.value
    case Equality(l: Const[_], r: RTColumn[U, _]) => l.value == r.expr(right)

    case And(l, r) =>
      new SeqOperatorEvaluator(l).evaluate(left, right) && new SeqOperatorEvaluator(r).evaluate(left, right)

    case x => throw new UnsupportedOperationException(s"Failed to evaluate expression $x")
  }
}
