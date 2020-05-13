package joinwiz

import java.sql.{Date, Timestamp}

import joinwiz.dataset.GrouppedByKeySyntax
import joinwiz.expression.ExpressionSyntax

import scala.language.{higherKinds, implicitConversions, postfixOps}
import scala.reflect.runtime.universe.TypeTag

object syntax extends ExpressionSyntax {

  type JOIN_CONDITION[L, R] = (ApplyToLeftColumn[L], ApplyToRightColumn[R]) => Expression

  implicit val dateOrdering: Ordering[Date] = new Ordering[Date] {
    override def compare(x: Date, y: Date): Int = Ordering[Long].compare(x.getTime, y.getTime)
  }

  implicit val timestampOrdering: Ordering[Timestamp] = new Ordering[Timestamp] {
    override def compare(x: Timestamp, y: Timestamp): Int = Ordering[Long].compare(x.getTime, y.getTime)
  }

  implicit class DatasetOperationsSyntax[F[_]: DatasetOperations, T](ft: F[T]) {
    def innerJoin[U](fu: F[U])(expr: JOIN_CONDITION[T, U]): F[(T, U)] =
      implicitly[DatasetOperations[F]].join.inner(ft, fu)(expr)

    def leftJoin[U](fu: F[U])(expr: JOIN_CONDITION[T, U]): F[(T, U)] =
      implicitly[DatasetOperations[F]].join.left(ft, fu)(expr)

    def map[U: TypeTag](func: T => U): F[U] =
      implicitly[DatasetOperations[F]].map(ft)(func)

    def flatMap[U: TypeTag](func: T => Seq[U]): F[U] =
      implicitly[DatasetOperations[F]].flatMap(ft)(func)

    def filter(func: T => Boolean): F[T] =
      implicitly[DatasetOperations[F]].filter(ft)(func)

    def distinct(): F[T] = implicitly[DatasetOperations[F]].distinct(ft)

    def groupByKey[K: TypeTag](func: T => K): GrouppedByKeySyntax[F, T, K] =
      implicitly[DatasetOperations[F]].groupByKey(ft)(func)

    def unionByName(other: F[T]): F[T] =
      implicitly[DatasetOperations[F]].unionByName(ft)(other)
  }

}
