package joinwiz.api

trait UnionByName[F[_]] {
  def apply[T](ft1: F[T])(ft2: F[T]): F[T]
}
