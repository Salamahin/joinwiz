package joinwiz.api

trait Filter[F[_]] {
  def apply[T](ft: F[T])(predicate: T => Boolean): F[T]
}
