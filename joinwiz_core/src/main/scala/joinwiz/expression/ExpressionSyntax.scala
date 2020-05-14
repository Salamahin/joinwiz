package joinwiz.expression

import joinwiz._

import scala.language.higherKinds

trait ExpressionSyntax {
  sealed trait ComparisionSyntax[F[_], T] {
    protected val thisCol: F[T]
    protected val e1: F[T] <:< TypedCol

    def =:=(c: T)                                                = Equality(e1(thisCol), Const(c))
    def =:=[G[_]](thatCol: G[T])(implicit e2: G[T] <:< TypedCol) = Equality(e1(thisCol), thatCol)

    def <(c: T)                                                = Less(e1(thisCol), Const(c))
    def <[G[_]](thatCol: G[T])(implicit e2: G[T] <:< TypedCol) = Less(e1(thisCol), thatCol)

    def >(c: T)                                                = Greater(e1(thisCol), Const(c))
    def >[G[_]](thatCol: G[T])(implicit e2: G[T] <:< TypedCol) = Greater(e1(thisCol), thatCol)

    def <=(c: T)                                                = LessOrEq(e1(thisCol), Const(c))
    def <=[G[_]](thatCol: G[T])(implicit e2: G[T] <:< TypedCol) = LessOrEq(e1(thisCol), thatCol)

    def >=(c: T)                                                = GreaterOrEq(e1(thisCol), Const(c))
    def >=[G[_]](thatCol: G[T])(implicit e2: G[T] <:< TypedCol) = GreaterOrEq(e1(thisCol), thatCol)
  }

  implicit class LeftTypedColumnComparisionSyntax[T: Ordering](left: LeftTypedColumn[T])
      extends ComparisionSyntax[LeftTypedColumn, T] {
    protected override val thisCol = left
    protected override val e1      = implicitly[LeftTypedColumn[T] <:< TypedCol]

    def some = LeftTypedColumn[Option[T]](left.prefixes)
  }

  implicit class RightTypedColumnComparisionSyntax[T: Ordering](right: RightTypedColumn[T])
      extends ComparisionSyntax[RightTypedColumn, T] {

    protected override val thisCol = right
    protected override val e1      = implicitly[RightTypedColumn[T] <:< TypedCol]

    def some = RightTypedColumn[Option[T]](right.prefixes)
  }

  implicit class ExpressionSyntax(expr: Expression) {
    def &&(other: Expression) = And(expr, other)
  }
}
