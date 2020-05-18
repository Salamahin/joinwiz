package joinwiz.dataset

trait Collect[F[_], T] {
  def apply(ft: F[T]): Seq[T]
}
