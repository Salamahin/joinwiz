package joinwiz.api

trait UnionByName[F[_], T] {
  def apply(ft1: F[T])(ft2: F[T]): F[T]
}
