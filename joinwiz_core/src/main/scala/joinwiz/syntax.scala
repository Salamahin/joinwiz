package joinwiz

import joinwiz.api.KeyValueGroupped
import joinwiz.expression._

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

object syntax extends EqualSyntax with CompareSyntax with CombinatorsSyntax with MapSyntax with IsEmptySyntax with Wrappers with UnapplySyntax with ExtractTColSyntax {

  type JOIN_CONDITION[L, R] = (ApplyLTCol[L, R, L], ApplyRTCol[L, R, R]) => Expr[L, R]

  implicit class DatasetLikeSyntax[F[_]: ComputationEngine, T: ClassTag](ft: F[T]) {
    def innerJoin[U](fu: F[U])(expr: JOIN_CONDITION[T, U]): F[(T, U)] =
      implicitly[ComputationEngine[F]].join.inner(ft, fu)(expr)

    def leftJoin[U](fu: F[U])(expr: JOIN_CONDITION[T, U])(implicit tt: TypeTag[(T, Option[U])]): F[(T, Option[U])] =
      implicitly[ComputationEngine[F]].join.left(ft, fu)(expr)

    def map[U: TypeTag](func: T => U): F[U] =
      implicitly[ComputationEngine[F]].map(ft)(func)

    def flatMap[U: TypeTag](func: T => Seq[U]): F[U] =
      implicitly[ComputationEngine[F]].flatMap(ft)(func)

    def filter(func: T => Boolean): F[T] =
      implicitly[ComputationEngine[F]].filter(ft)(func)

    def distinct(): F[T] = implicitly[ComputationEngine[F]].distinct(ft)

    def groupByKey[K: TypeTag](func: T => K): KeyValueGroupped[F, T, K] =
      implicitly[ComputationEngine[F]].groupByKey(ft)(func)

    def unionByName(other: F[T]): F[T] =
      implicitly[ComputationEngine[F]].unionByName(ft)(other)

    def collect(): Seq[T] = implicitly[ComputationEngine[F]].collect(ft)
  }
}
