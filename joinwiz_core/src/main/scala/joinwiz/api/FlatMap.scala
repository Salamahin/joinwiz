package joinwiz.api

import scala.reflect.runtime.universe.TypeTag

trait FlatMap[F[_]] {
  def apply[T, U: TypeTag](ft: F[T])(func: T => TraversableOnce[U]): F[U]
}
