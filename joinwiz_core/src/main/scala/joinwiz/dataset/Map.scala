package joinwiz.dataset

import scala.reflect.runtime.universe.TypeTag

trait Map[F[_], T] {
  def apply[U: TypeTag](ft: F[T])(func: T => U): F[U]
}
