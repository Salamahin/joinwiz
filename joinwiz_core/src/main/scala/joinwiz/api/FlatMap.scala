package joinwiz.api

import scala.reflect.runtime.universe.TypeTag

trait FlatMap[F[_], T] {
  def apply[U: TypeTag](ft: F[T])(func: T => TraversableOnce[U]): F[U]
}
