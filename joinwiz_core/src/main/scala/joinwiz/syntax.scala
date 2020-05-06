package joinwiz

import joinwiz.ops.GrouppedByKeySyntax

import scala.language.{higherKinds, implicitConversions, postfixOps}
import scala.reflect.runtime.universe.TypeTag

object syntax {

  type JOIN_CONDITION[L, R] = (ApplyToLeftColumn[L, L], ApplyToRightColumn[R, R]) => Expression

  implicit class DatasetOperationsSyntax[F[_]: DatasetOperations, T](ft: F[T]) {
    def innerJoin[U](fu: F[U])(expr: JOIN_CONDITION[T, U]): F[(T, U)] =
      implicitly[DatasetOperations[F]].join.inner(ft, fu)(expr)

    def leftJoin[U](fu: F[U])(expr: JOIN_CONDITION[T, U]): F[(T, U)] =
      implicitly[DatasetOperations[F]].join.left(ft, fu)(expr)

    def map[U <: Product: TypeTag](func: T => U): F[U] =
      implicitly[DatasetOperations[F]].map(ft)(func)

    def flatMap[U <: Product: TypeTag](func: T => Seq[U]): F[U] =
      implicitly[DatasetOperations[F]].flatMap(ft)(func)

    def filter(func: T => Boolean): F[T] =
      implicitly[DatasetOperations[F]].filter(ft)(func)

    def distinct(): F[T] = implicitly[DatasetOperations[F]].distinct(ft)

    def groupByKey[K <: Product: TypeTag](func: T => K): GrouppedByKeySyntax[F, T, K] =
      implicitly[DatasetOperations[F]].groupByKey(ft)(func)
  }

  abstract class EqualitySyntax[L[_, _, _], T](thisCol: L[_, _, T])(implicit e1: L[_, _, T] <:< TypedCol) {
    def =:=(c: T)                                                                  = Equality(thisCol, Const(c))
    def =:=[G[_, _, _]](thatCol: G[_, _, T])(implicit e2: G[_, _, T] <:< TypedCol) = Equality(thisCol, thatCol)
  }

  implicit class LeftTypedColumnSyntax[LO, L, T](left: LeftTypedColumn[LO, L, T])
      extends EqualitySyntax[LeftTypedColumn, T](left) {

    def some = new LeftTypedColumn[LO, L, Option[T]] {
      override def apply(source: LO): Option[T] = Some(left.apply(source))
      override val name: String                 = left.name
    }
  }

  implicit class RightTypedColumnSyntax[RO, R, T](right: RightTypedColumn[RO, R, T])
      extends EqualitySyntax[RightTypedColumn, T](right) {

    def some = new RightTypedColumn[RO, R, Option[T]] {
      override def apply(source: RO): Option[T] = Some(right.apply(source))
      override val name: String                 = right.name
    }
  }

  implicit class ExpressionSyntax(expr: Expression) {
    def &&(other: Expression) = And(expr, other)
  }
}
