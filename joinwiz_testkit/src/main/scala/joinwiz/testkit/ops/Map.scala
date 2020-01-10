package joinwiz.testkit.ops

import scala.language.higherKinds
import scala.reflect.runtime.universe.TypeTag

trait Map[F[_], T] {
  def apply[U <: Product : TypeTag](ft: F[T])(func: T => U): F[U]
}
