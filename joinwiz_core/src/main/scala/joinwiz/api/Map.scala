package joinwiz.api

import scala.reflect.runtime.universe.TypeTag

trait Map[F[_]] {
  def apply[T, U: TypeTag](ft: F[T])(func: T => U): F[U]
}
