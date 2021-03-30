package joinwiz.api

trait Collect[F[_]] {
  def apply[T](ft: F[T]): Seq[T]
}
