package joinwiz.expression

import joinwiz.{ApplyTColumnSyntax, LTColumn, RTColumn}

trait Tupled[F[_, _, _]] {
  def left[L, R, A, B](t: F[L, R, (A, B)]): F[L, R, A]
  def right[L, R, A, B](t: F[L, R, (A, B)]): F[L, R, B]
}

object Tupled extends ApplyTColumnSyntax {
  implicit val applyToLeftTupled: Tupled[LTColumn] = new Tupled[LTColumn] {
    override def left[L, R, A, B](t: LTColumn[L, R, (A, B)]): LTColumn[L, R, A]  = t >> (_._1)
    override def right[L, R, A, B](t: LTColumn[L, R, (A, B)]): LTColumn[L, R, B] = t >> (_._2)
  }

  implicit val applyToRightTupled: Tupled[RTColumn] = new Tupled[RTColumn] {
    override def left[L, R, A, B](t: RTColumn[L, R, (A, B)]): RTColumn[L, R, A]  = t >> (_._1)
    override def right[L, R, A, B](t: RTColumn[L, R, (A, B)]): RTColumn[L, R, B] = t >> (_._2)
  }
}

trait UnapplySyntax2 {
  object wiz {
    def unapply[F[_, _, _]: Tupled, L, R, A, B](tupled: F[L, R, (A, B)])(implicit t: Tupled[F]): Option[(F[L, R, A], F[L, R, B])] = Some(t.left(tupled), t.right(tupled))
  }
}
