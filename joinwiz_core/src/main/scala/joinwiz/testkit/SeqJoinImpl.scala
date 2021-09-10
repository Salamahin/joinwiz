package joinwiz.testkit

import joinwiz.Expr

class SeqJoinImpl[L, R](op: Expr[L, R], left: Seq[L], right: Seq[R]) {

  def leftJoin() = {
    val joined          = innerJoin()
    val (leftJoined, _) = joined.unzip
    val notJoined       = left diff leftJoined

    (joined ++ notJoined.map((_, null.asInstanceOf[R])))
      .map {
        case (x, y) => (x, Option(y))
      }
  }

  def innerJoin(): Seq[(L, R)] =
    for {
      a <- left
      b <- right
      if op(a, b)
    } yield (a, b)

  def leftAntiJoin(): Seq[L] = {
    leftJoin()
      .filter { case (_, right_) => right_.isEmpty }
      .map { case (left_, _) => left_ }
  }

}
