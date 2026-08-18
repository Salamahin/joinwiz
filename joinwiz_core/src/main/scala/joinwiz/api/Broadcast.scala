package joinwiz.api

trait Broadcast[F[_]] {
  def apply[T](ft: F[T]): F[T]
}
