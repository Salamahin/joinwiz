package joinwiz.dataset

import scala.language.higherKinds
import scala.reflect.runtime.universe.TypeTag

trait GroupByKey[F[_], T] {
  def apply[K: TypeTag](ft: F[T])(func: T => K): GrouppedByKeySyntax[F, T, K]
}

trait GrouppedByKeySyntax[F[_], T, K] {
  def mapGroups[U: TypeTag](f: (K, Iterator[T]) => U): F[U]
}
