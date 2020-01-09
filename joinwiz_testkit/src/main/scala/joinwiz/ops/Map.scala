package joinwiz.ops

trait Map[F[_], T] {
  def map[U <: Product : reflect.runtime.universe.TypeTag](ft: F[T])(func: T => U): F[U]
}
