package joinwiz.testkit.sparkless

import joinwiz.Operator

class SeqJoinImpl[T, U](op: Operator, s1: Seq[T], s2: Seq[U]) {
  private val eval = new SeqOperatorEvaluator[T, U](op)

  def innerJoin(): Seq[(T, U)] = for {
    a <- s1
    b <- s2
    if eval.evaluate(a, b)
  } yield (a, b)

  def leftJoin(): Seq[(T, U)] = {
    val result = for {
      a <- s1
      b <- s2
      c = if (eval.evaluate(a, b)) b else null.asInstanceOf[U]
    } yield (a, c)

    result.distinct
  }
}
