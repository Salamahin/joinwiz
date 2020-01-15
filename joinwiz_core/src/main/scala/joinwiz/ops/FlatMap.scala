package joinwiz.ops

import scala.language.higherKinds
import scala.reflect.runtime.universe.TypeTag

trait FlatMap[F[_], T] {
  def apply[U <: Product : TypeTag](ft: F[T])(func: T => TraversableOnce[U]): F[U]
}
