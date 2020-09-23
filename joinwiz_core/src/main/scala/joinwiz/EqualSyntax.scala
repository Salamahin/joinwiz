package joinwiz

import joinwiz.Expr.expr

final case class LTCol2W[O, T] private[joinwiz] (wrapped: LTCol2[O, T])
final case class RTCol2W[O, T] private[joinwiz] (wrapped: RTCol2[O, T])
final case class LTCol2OptW[O, T] private[joinwiz] (wrapped: LTCol2[O, Option[T]])
final case class RTCol2OptW[O, T] private[joinwiz] (wrapped: RTCol2[O, Option[T]])
final case class ConstW[T] private[joinwiz] (wrapped: T)
final case class OptConstW[T] private[joinwiz] (wrapped: Option[T])

trait EqualSyntax {
  import org.apache.spark.sql.functions.lit

  implicit def ltColToWrapper[O, T](col: LTCol2[O, T])            = LTCol2W[O, T](col)
  implicit def rtColToWrapper[O, T](col: RTCol2[O, T])            = RTCol2W[O, T](col)
  implicit def ltOptColToWrapper[O, T](col: LTCol2[O, Option[T]]) = LTCol2OptW[O, T](col)
  implicit def rtOptColToWrapper[O, T](col: RTCol2[O, Option[T]]) = RTCol2OptW[O, T](col)
  implicit def constWrapper[T](const: T)                          = ConstW[T](const)
  implicit def optConstWrapper[T](const: Option[T])               = OptConstW[T](const)

  implicit class LTCol2EqualSyntax[L, T](thisCol: LTCol2[L, T]) {
    def =:=[R](thatCol: LTCol2W[L, T]): Expr[L, R]    = expr[L, R](thisCol.column === thatCol.wrapped.column)((l, _) => thisCol(l) == thatCol.wrapped(l))
    def =:=[R](thatCol: RTCol2W[R, T]): Expr[L, R]    = expr[L, R](thisCol.column === thatCol.wrapped.column)((l, r) => thisCol(l) == thatCol.wrapped(r))
    def =:=[R](thatCol: LTCol2OptW[L, T]): Expr[L, R] = expr[L, R](thisCol.column === thatCol.wrapped.column)((l, _) => thatCol.wrapped(l) contains thisCol(l))
    def =:=[R](thatCol: RTCol2OptW[R, T]): Expr[L, R] = expr[L, R](thisCol.column === thatCol.wrapped.column)((l, r) => thatCol.wrapped(r) contains thisCol(l))
    def =:=[R](const: ConstW[T]): Expr[L, R]          = expr[L, R](thisCol.column === lit(const))((l, _) => thisCol(l) == const.wrapped)
  }

  implicit class RTCol2EqualSyntax[R, T](thisCol: RTCol2[R, T]) {
    def =:=[L](thatCol: LTCol2W[L, T]): Expr[L, R]    = expr[L, R](thisCol.column === thatCol.wrapped.column)((l, r) => thisCol(r) == thatCol.wrapped(l))
    def =:=[L](thatCol: RTCol2W[R, T]): Expr[L, R]    = expr[L, R](thisCol.column === thatCol.wrapped.column)((_, r) => thisCol(r) == thatCol.wrapped(r))
    def =:=[L](thatCol: RTCol2OptW[R, T]): Expr[L, R] = expr[L, R](thisCol.column === thatCol.wrapped.column)((_, r) => thatCol.wrapped(r) contains thisCol(r))
    def =:=[L](thatCol: LTCol2OptW[L, T]): Expr[L, R] = expr[L, R](thisCol.column === thatCol.wrapped.column)((l, r) => thatCol.wrapped(l) contains thisCol(r))
    def =:=[L](const: ConstW[T]): Expr[L, R]          = expr[L, R](thisCol.column === lit(const))((_, r) => thisCol(r) == const)
  }
}

trait OptionEqualSyntax extends EqualSyntax {
  import org.apache.spark.sql.functions.lit

  implicit class OptionalLTCol2EqualSyntax[L, T](thisCol: LTCol2[L, Option[T]]) {
    def =:=[R](thatCol: LTCol2W[L, T]): Expr[L, R]    = expr[L, R](thisCol.column === thatCol.wrapped.column)((l, _) => thisCol(l) contains thatCol.wrapped(l))
    def =:=[R](thatCol: RTCol2W[R, T]): Expr[L, R]    = expr[L, R](thisCol.column === thatCol.wrapped.column)((l, r) => thisCol(l) contains thatCol.wrapped(r))
    def =:=[R](thatCol: LTCol2OptW[L, T]): Expr[L, R] = expr[L, R](thisCol.column === thatCol.wrapped.column)((l, _) => thatCol.wrapped(l) == thisCol.wrapped(l))
    def =:=[R](thatCol: RTCol2OptW[R, T]): Expr[L, R] = expr[L, R](thisCol.column === thatCol.wrapped.column)((l, r) => thatCol.wrapped(r) == thisCol(l))
    def =:=[R](const: ConstW[T]): Expr[L, R]          = expr[L, R](thisCol.column === lit(const))((l, _) => thisCol(l) contains const.wrapped)
  }

  implicit class OptionalRTCol2EqualOptionSyntax[R, T](thisCol: RTCol2[R, Option[T]]) {
    def =:=[L](thatCol: LTCol2W[L, T]): Expr[L, R]    = expr[L, R](thisCol.column === thatCol.wrapped.column)((l, r) => thisCol(r) contains thatCol.wrapped(l))
    def =:=[L](thatCol: RTCol2W[R, T]): Expr[L, R]    = expr[L, R](thisCol.column === thatCol.wrapped.column)((_, r) => thisCol(r) contains thatCol.wrapped(r))
    def =:=[L](thatCol: RTCol2OptW[R, T]): Expr[L, R] = expr[L, R](thisCol.column === thatCol.wrapped.column)((_, r) => thatCol.wrapped(r) == thisCol.wrapped(r))
    def =:=[L](thatCol: LTCol2OptW[L, T]): Expr[L, R] = expr[L, R](thisCol.column === thatCol.wrapped.column)((l, r) => thatCol.wrapped(l) == thisCol.wrapped(r))
    def =:=[L](const: ConstW[T]): Expr[L, R]          = expr[L, R](thisCol.column === lit(const))((_, r) => thisCol(r) contains const.wrapped)
  }
}

object syntax2 extends OptionEqualSyntax

object TestRunner extends App {
  import joinwiz.syntax2._

  case class A(pk: String, optional: Option[String])
  case class B(pk: String, optional: Option[String])

  val testee: (ApplyLTCol2[A, A], ApplyRTCol2[B, B]) = ???

  testee match {
    case (left, right) => left(_.pk) =:= right(_.pk)
  }

  testee match {
    case (left, right) => left(_.pk) =:= right(_.optional)
  }

  testee match {
    case (left, right) => left(_.optional) =:= right(_.optional)
  }

  testee match {
    case (left, _) => left(_.pk) =:= "1"
  }

  testee match {
    case (left, _) => left(_.optional) =:= "1"
  }

}
