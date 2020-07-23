package joinwiz

object wiz {
  def unapply[F[_, _]: Tupled, O, A, B](tupled: F[O, (A, B)]): Option[(F[O, A], F[O, B])] = {
    val t = implicitly[Tupled[F]]
    Some(t.left(tupled), t.right(tupled))
  }

  trait Tupled[F[_, _]] {
    def left[O, A, B](t: F[O, (A, B)]): F[O, A]
    def right[O, A, B](t: F[O, (A, B)]): F[O, B]
  }

  implicit val applyToLeftTupled = new Tupled[ApplyLeft] {
    override def left[O, A, B](t: ApplyLeft[O, (A, B)]): ApplyLeft[O, A]  = t.map("_1", _._1)
    override def right[O, A, B](t: ApplyLeft[O, (A, B)]): ApplyLeft[O, B] = t.map("_2", _._2)
  }

  implicit val applyToRightTupled = new Tupled[ApplyRight] {
    override def left[O, A, B](t: ApplyRight[O, (A, B)]): ApplyRight[O, A]  = t.map("_1", _._1)
    override def right[O, A, B](t: ApplyRight[O, (A, B)]): ApplyRight[O, B] = t.map("_2", _._2)
  }
}
