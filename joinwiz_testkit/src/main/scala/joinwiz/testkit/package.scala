package joinwiz

import scala.language.higherKinds
import scala.reflect.runtime.universe.TypeTag

package object testkit {

  implicit class DatasetOperationsSyntax[F[_] : DatasetOperations, T](ft: F[T]) {
    def innerJoin[U](fu: F[U])(expr: JOIN_CONDITION[T, U]): F[(T, U)] =
      implicitly[DatasetOperations[F]].join.inner(ft, fu)(expr)

    def leftJoin[U](fu: F[U])(expr: JOIN_CONDITION[T, U]): F[(T, U)] =
      implicitly[DatasetOperations[F]].join.left(ft, fu)(expr)

    def map[U <: Product : TypeTag](func: T => U): F[U] =
      implicitly[DatasetOperations[F]].map(ft)(func)

    def flatMap[U <: Product : TypeTag](func: T => Seq[U]): F[U] =
      implicitly[DatasetOperations[F]].flatMap(ft)(func)

    def filter(func: T => Boolean): F[T] =
      implicitly[DatasetOperations[F]].filter(ft)(func)
  }

}
