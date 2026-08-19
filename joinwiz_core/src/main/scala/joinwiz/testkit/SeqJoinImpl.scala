package joinwiz.testkit

import joinwiz.expression.JoinCondition

class SeqJoinImpl[L, R](op: JoinCondition[L, R], left: Seq[L], right: Seq[R]) {

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
    leftJoin().collect { case (left, None) => left }
  }

  def leftSemiJoin(): Seq[L] =
    left.filter(l => right.exists(r => op(l, r)))

  def rightJoin(): Seq[(Option[L], R)] = {
    val joined            = innerJoin()
    val (_, rightJoined)  = joined.unzip
    val notJoined         = right diff rightJoined

    joined.map { case (l, r) => (Option(l), r) } ++ notJoined.map(r => (Option.empty[L], r))
  }

  def fullJoin(): Seq[(Option[L], Option[R])] = {
    val joined                 = innerJoin()
    val (leftJoined, rightJoined) = joined.unzip
    val leftOnly               = (left diff leftJoined).map(l => (Option(l), Option.empty[R]))
    val rightOnly              = (right diff rightJoined).map(r => (Option.empty[L], Option(r)))

    joined.map { case (l, r) => (Option(l), Option(r)) } ++ leftOnly ++ rightOnly
  }

}
