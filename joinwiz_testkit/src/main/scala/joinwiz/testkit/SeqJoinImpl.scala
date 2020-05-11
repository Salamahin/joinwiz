package joinwiz.testkit

import joinwiz.Expression

class SeqJoinImpl[T, U](op: Expression, left: Seq[T], right: Seq[U]) {
  private val eval = new SeqExpressionEvaluator[T, U]

  def innerJoin(): Seq[(T, U)] =
    for {
      a <- left
      b <- right
      if eval(a, b).evaluate(op)
    } yield (a, b)

  def leftJoin(): Seq[(T, U)] = {
    val joined = innerJoin()
    val (leftJoined, _) = joined.unzip
    val notJoined = left diff leftJoined

    joined ++ notJoined.map((_, null.asInstanceOf[U]))
  }
}
