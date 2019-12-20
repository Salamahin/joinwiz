package joinwiz

import joinwiz.JoinWiz.JOIN_CONDITION

import scala.language.higherKinds

trait JoinWizLike[F[_], T, U] {
  def innerJoin(ft: F[T], fu: F[U])(expr: JOIN_CONDITION[T, U]): F[(T, U)]
}

object JoinWizLike {
  implicit def seqJoinWizLike[T, U]: JoinWizLike[Seq, T, U] = new JoinWizLike[Seq, T, U] {
    override def innerJoin(ft: Seq[T], fu: Seq[U])(expr: JOIN_CONDITION[T, U]): Seq[(T, U)] = {
      new SeqJoinOperator[T, U](expr(new LTColumnExtractor[T], new RTColumnExtractor[U]), ft, fu).innerJoin()
    }
  }
}
