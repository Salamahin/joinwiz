package joinwiz.api

trait Collect[F[_], T] {
  def apply(ft: F[T]): Seq[T]
}
