package joinwiz.expression

import joinwiz.{ApplyLTCol, ApplyRTCol}

@deprecated
trait UnapplySyntax {

  object wiz {
    def unapply[F[_, _, _]: Tupled, L, R, A, B](tupled: F[L, R, (A, B)]): Option[(F[L, R, A], F[L, R, B])] = {
      val t = implicitly[Tupled[F]]
      Some(t.left(tupled), t.right(tupled))
    }

    trait Tupled[F[_, _, _]] {
      def left[L, R, A, B](t: F[L, R, (A, B)]): F[L, R, A]
      def right[L, R, A, B](t: F[L, R, (A, B)]): F[L, R, B]
    }
  }

  import wiz._

  implicit val applyToLeftTupled: Tupled[ApplyLTCol] = new Tupled[ApplyLTCol] {
    override def left[L, R, A, B](t: ApplyLTCol[L, R, (A, B)]): ApplyLTCol[L, R, A] = t.map("_1", _._1)

    override def right[L, R, A, B](t: ApplyLTCol[L, R, (A, B)]): ApplyLTCol[L, R, B] = t.map("_2", _._2)
  }

  implicit val applyToRightTupled: Tupled[ApplyRTCol] = new Tupled[ApplyRTCol] {
    override def left[L, R, A, B](t: ApplyRTCol[L, R, (A, B)]): ApplyRTCol[L, R, A] = t.map("_1", _._1)

    override def right[L, R, A, B](t: ApplyRTCol[L, R, (A, B)]): ApplyRTCol[L, R, B] = t.map("_2", _._2)
  }

}
