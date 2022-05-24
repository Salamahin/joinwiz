package joinwiz.api

import scala.collection.Iterable
import scala.reflect.runtime.universe.TypeTag

trait FlatMap[F[_]] {
  def apply[T, U: TypeTag](ft: F[T])(func: T => Iterable[U]): F[U]
}
