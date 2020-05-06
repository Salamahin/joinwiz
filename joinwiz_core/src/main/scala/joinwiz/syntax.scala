package joinwiz

import joinwiz.dataset.GrouppedByKeySyntax

import scala.language.{higherKinds, implicitConversions, postfixOps}
import scala.reflect.runtime.universe.TypeTag

sealed trait ExpressionSyntaxes {
  sealed trait EqualitySyntax[F[_, _, _], LO, L, T] {
    protected val thisCol: F[LO, L, T]
    protected val e1: F[LO, L, T] <:< TypedCol

    def =:=(c: T)                                                                  = Equality(e1(thisCol), Const(c))
    def =:=[G[_, _, _]](thatCol: G[_, _, T])(implicit e2: G[_, _, T] <:< TypedCol) = Equality(e1(thisCol), thatCol)
  }

  sealed trait ComparisionSyntax[F[_, _, _], LO, L, T] {
    protected val thisCol: F[LO, L, T]
    protected val e1: F[LO, L, T] <:< TypedCol

    def <(c: T)                                                                  = Less(e1(thisCol), Const(c))
    def <[G[_, _, _]](thatCol: G[_, _, T])(implicit e2: G[_, _, T] <:< TypedCol) = Less(e1(thisCol), thatCol)

    def >(c: T)                                                                  = Greater(e1(thisCol), Const(c))
    def >[G[_, _, _]](thatCol: G[_, _, T])(implicit e2: G[_, _, T] <:< TypedCol) = Greater(e1(thisCol), thatCol)

    def <=(c: T)                                                                  = LessOrEq(e1(thisCol), Const(c))
    def <=[G[_, _, _]](thatCol: G[_, _, T])(implicit e2: G[_, _, T] <:< TypedCol) = LessOrEq(e1(thisCol), thatCol)

    def >=(c: T)                                                                  = GreaterOrEq(e1(thisCol), Const(c))
    def >=[G[_, _, _]](thatCol: G[_, _, T])(implicit e2: G[_, _, T] <:< TypedCol) = GreaterOrEq(e1(thisCol), thatCol)
  }

  implicit class LeftTypedColumnComparisionSyntax[LO, L, T: Ordering](left: LeftTypedColumn[LO, L, T])
      extends ComparisionSyntax[LeftTypedColumn, LO, L, T] {
    protected override val thisCol = left
    protected override val e1      = implicitly[LeftTypedColumn[LO, L, T] <:< TypedCol]
  }

  implicit class RightTypedColumnComparisionSyntax[RO, R, T](right: RightTypedColumn[RO, R, T])
      extends ComparisionSyntax[RightTypedColumn, RO, R, T] {

    protected override val thisCol = right
    protected override val e1      = implicitly[RightTypedColumn[RO, R, T] <:< TypedCol]
  }

  implicit class LeftTypedColumnSyntax[LO, L, T](left: LeftTypedColumn[LO, L, T])
      extends EqualitySyntax[LeftTypedColumn, LO, L, T] {

    def some = new LeftTypedColumn[LO, L, Option[T]] {
      override def apply(source: LO): Option[T] = Some(left.apply(source))
      override val name: String                 = left.name
    }

    protected override val thisCol = left
    protected override val e1      = implicitly[LeftTypedColumn[LO, L, T] <:< TypedCol]
  }

  implicit class RightTypedColumnSyntax[RO, R, T](right: RightTypedColumn[RO, R, T])
      extends EqualitySyntax[RightTypedColumn, RO, R, T] {

    def some = new RightTypedColumn[RO, R, Option[T]] {
      override def apply(source: RO): Option[T] = Some(right.apply(source))
      override val name: String                 = right.name
    }

    protected override val thisCol = right
    protected override val e1      = implicitly[RightTypedColumn[RO, R, T] <:< TypedCol]
  }

  implicit class ExpressionSyntax(expr: Expression) {
    def &&(other: Expression) = And(expr, other)
  }
}

object syntax extends ExpressionSyntaxes {

  type JOIN_CONDITION[L, R] = (ApplyToLeftColumn[L, L], ApplyToRightColumn[R, R]) => Expression

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
  }

}
