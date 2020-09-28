package joinwiz.expression

trait Functor[F[_]] {
  def pure[T](t: T): F[T]
  def map[T, U](ft: F[T])(f: T => U): F[U]
}

object Functor {
  type Id[T] = T

  implicit val optionPure: Functor[Option] = new Functor[Option] {
    override def pure[T](t: T): Option[T]                       = Option(t)
    override def map[T, U](ft: Option[T])(f: T => U): Option[U] = ft map f
  }

  implicit val idPure: Functor[Id] = new Functor[Id] {
    override def pure[T](t: T): Id[T]                   = t
    override def map[T, U](ft: Id[T])(f: T => U): Id[U] = f(ft)
  }
}
