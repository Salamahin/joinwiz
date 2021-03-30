package joinwiz.api

trait Distinct[F[_]] {
  def apply[T](ft: F[T]): F[T]
}
