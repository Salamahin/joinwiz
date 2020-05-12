package joinwiz

import scala.language.higherKinds

object joined {
  def unapply[F[_]: Tupled, A, B](tupled: F[(A, B)]): Option[(F[A], F[B])] = {
    val t = implicitly[Tupled[F]]
    Some(t.left(tupled), t.right(tupled))
  }

  trait Tupled[F[_]] {
    def left[A, B](t: F[(A, B)]): F[A]
    def right[A, B](t: F[(A, B)]): F[B]
  }

  implicit val applyToLeftTupled = new Tupled[ApplyToLeftColumn] {
    override def left[A, B](t: ApplyToLeftColumn[(A, B)]): ApplyToLeftColumn[A] =
      new ApplyToLeftColumn[A](t.prefixes :+ "_1")

    override def right[A, B](t: ApplyToLeftColumn[(A, B)]): ApplyToLeftColumn[B] =
      new ApplyToLeftColumn[B](t.prefixes :+ "_2")
  }

  implicit val applyToRightTupled = new Tupled[ApplyToRightColumn] {
    override def left[A, B](t: ApplyToRightColumn[(A, B)]): ApplyToRightColumn[A] =
      new ApplyToRightColumn[A](t.prefixes :+ "_1")

    override def right[A, B](t: ApplyToRightColumn[(A, B)]): ApplyToRightColumn[B] =
      new ApplyToRightColumn[B](t.prefixes :+ "_2")
  }

}
