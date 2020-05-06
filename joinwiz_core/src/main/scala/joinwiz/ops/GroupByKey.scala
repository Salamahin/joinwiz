package joinwiz.ops

import org.apache.spark.sql.Encoder

import scala.language.higherKinds

trait GroupByKey[F[_], T] {
  def apply[K: Encoder](ft: F[T])(func: T => K): GrouppedByKeyOps[F, T, K]
}

trait GrouppedByKeyOps[F[_], T, K] {
  def mapGroups[U: Encoder](f: (K, Iterator[T]) => U): F[U]
}
