package joinwiz.api

trait Distinct[F[_], T] {
  def apply(ft: F[T]): F[T]
}
