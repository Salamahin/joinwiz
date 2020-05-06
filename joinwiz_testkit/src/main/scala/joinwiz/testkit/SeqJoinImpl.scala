package joinwiz.testkit

import joinwiz.Expression

class SeqJoinImpl[T, U](op: Expression, s1: Seq[T], s2: Seq[U]) {
  private val eval = new SeqExpressionEvaluator[T, U]

  def innerJoin(): Seq[(T, U)] =
    for {
      a <- s1
      b <- s2
      if eval(a, b).evaluate(op)
    } yield (a, b)

  def leftJoin(): Seq[(T, U)] = {
    val result = for {
      a <- s1
      b <- s2
      c = if (eval(a, b).evaluate(op)) b else null.asInstanceOf[U]
    } yield (a, c)

    result.distinct
  }
}
