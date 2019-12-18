package joinwiz

import joinwiz.JoinWiz.JOIN_CONDITION

trait JoinWizLike[F[_], T, U] {
  def joinWiz(ft: F[T], fu: F[U])(expr: JOIN_CONDITION[T, U]): F[(T, U)]
}

object JoinWizLike {
//  implicit def seqJoinWizLike[T, U] = new JoinWizLike[Seq, T, U] {
//    override def joinWiz(ft: Seq[T], fu: Seq[U])(expr: JOIN_CONDITION[T, U]): Seq[(T, U)] = {
//
//    }
//  }
}
