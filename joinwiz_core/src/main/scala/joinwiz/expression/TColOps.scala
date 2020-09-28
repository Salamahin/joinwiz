package joinwiz.expression

trait TColOps[F[_]] {
  def pure[T](t: T): F[T]
  def map[T, U](ft: F[T])(f: T => U): F[U]

  def equalsToSame[T](thisFt: F[T], thatFt: T): Boolean
  def equalsToOption[T](thisFt: F[T], thatFt: Option[T]): Boolean
}

object TColOps {
  type Id[T] = T

  implicit val optionPure: TColOps[Option] = new TColOps[Option] {
    override def pure[T](t: T): Option[T]                       = Option(t)
    override def map[T, U](ft: Option[T])(f: T => U): Option[U] = ft map f

    override def equalsToSame[T](thisFt: Option[T], thatFt: T): Boolean            = ???
    override def equalsToOption[T](thisFt: Option[T], thatFt: Option[T]): Boolean   = ???
  }

  implicit val idPure: TColOps[Id] = new TColOps[Id] {
    override def pure[T](t: T): Id[T]                   = t
    override def map[T, U](ft: Id[T])(f: T => U): Id[U] = f(ft)

    override def equalsToSame[T](thisFt: Id[T], thatFt: Id[T]): Boolean   = ???
    override def equalsToOption[T](thisFt: Id[T], thatFt: Option[T]): Boolean = ???
  }
}
