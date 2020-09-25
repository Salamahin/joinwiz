package joinwiz.api

trait Filter[F[_], T] {
  def apply(ft: F[T])(predicate: T => Boolean): F[T]
}
