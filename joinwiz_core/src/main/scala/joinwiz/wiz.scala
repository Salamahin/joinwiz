package joinwiz

object wiz {
  def unapply[F[_]: Tupled, A, B](tupled: F[(A, B)]): Option[(F[A], F[B])] = {
    val t = implicitly[Tupled[F]]
    Some(t.left(tupled), t.right(tupled))
  }

  trait Tupled[F[_]] {
    def left[A, B](t: F[(A, B)]): F[A]
    def right[A, B](t: F[(A, B)]): F[B]
  }

  implicit val applyToLeftTupled = new Tupled[ApplyLeft] {
    override def left[A, B](t: ApplyLeft[(A, B)]): ApplyLeft[A] =
      new ApplyLeft[A](t.names :+ "_1")

    override def right[A, B](t: ApplyLeft[(A, B)]): ApplyLeft[B] =
      new ApplyLeft[B](t.names :+ "_2")
  }

  implicit val applyToRightTupled = new Tupled[ApplyRight] {
    override def left[A, B](t: ApplyRight[(A, B)]): ApplyRight[A] =
      new ApplyRight[A](t.names :+ "_1")

    override def right[A, B](t: ApplyRight[(A, B)]): ApplyRight[B] =
      new ApplyRight[B](t.names :+ "_2")
  }

}
