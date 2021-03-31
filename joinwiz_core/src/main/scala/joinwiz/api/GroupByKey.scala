package joinwiz.api

import scala.reflect.runtime.universe.TypeTag

trait GroupByKey[F[_]] {
  def apply[T, K: TypeTag](ft: F[T])(func: T => K): KeyValueGroupped[F, T, K]
}

trait KeyValueGroupped[F[_], T, K] {
  private[joinwiz] def underlying: F[T]
  private[joinwiz] def keyFunc: T => K

  def mapGroups[U: TypeTag](f: (K, Iterator[T]) => U): F[U]
  def reduceGroups(f: (T, T) => T): F[(K, T)]
  def count(): F[(K, Long)]
  def cogroup[U, R: TypeTag](other: KeyValueGroupped[F, U, K])(f: (K, Iterator[T], Iterator[U]) => TraversableOnce[R]): F[R]
}
