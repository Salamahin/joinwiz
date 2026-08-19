package joinwiz.expression

import joinwiz.{ApplyJoinColumnSyntax, LeftColumn, RightColumn}

trait Tupled[F[_, _, _]] {
  def left[L, R, A, B](t: F[L, R, (A, B)]): F[L, R, A]
  def right[L, R, A, B](t: F[L, R, (A, B)]): F[L, R, B]
}

object Tupled extends ApplyJoinColumnSyntax {
  implicit val applyToLeftTupled: Tupled[LeftColumn] = new Tupled[LeftColumn] {
    override def left[L, R, A, B](t: LeftColumn[L, R, (A, B)]): LeftColumn[L, R, A]  = t >> (_._1)
    override def right[L, R, A, B](t: LeftColumn[L, R, (A, B)]): LeftColumn[L, R, B] = t >> (_._2)
  }

  implicit val applyToRightTupled: Tupled[RightColumn] = new Tupled[RightColumn] {
    override def left[L, R, A, B](t: RightColumn[L, R, (A, B)]): RightColumn[L, R, A]  = t >> (_._1)
    override def right[L, R, A, B](t: RightColumn[L, R, (A, B)]): RightColumn[L, R, B] = t >> (_._2)
  }
}

trait UnapplySyntax {
  object wiz {
    def unapply[F[_, _, _]: Tupled, L, R, A, B](tupled: F[L, R, (A, B)])(implicit t: Tupled[F]): Option[(F[L, R, A], F[L, R, B])] = Some(t.left(tupled), t.right(tupled))
  }
}
