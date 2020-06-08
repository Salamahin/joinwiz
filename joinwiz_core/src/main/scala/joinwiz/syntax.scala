package joinwiz

import joinwiz.dataset.KeyValueGroupped

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

object syntax {

  type JOIN_CONDITION[L, R] = (ApplyLeft[L], ApplyRight[R]) => Expression

  implicit class DatasetLikeSyntax[F[_]: ComputationEngine, T: ClassTag](ft: F[T]) {
    def innerJoin[U](fu: F[U])(expr: JOIN_CONDITION[T, U]): F[(T, U)] =
      implicitly[ComputationEngine[F]].join.inner(ft, fu)(expr)

    def leftJoin[U](fu: F[U])(expr: JOIN_CONDITION[T, U]): F[(T, U)] =
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

  implicit class LTColSyntax[T: TypeTag](left: LTCol[_, T]) extends Serializable {
    def <(c: T)   = Less(left, Const(c))
    def >(c: T)   = Greater(left, Const(c))
    def <=(c: T)  = LessOrEq(left, Const(c))
    def >=(c: T)  = GreaterOrEq(left, Const(c))
    def =:=(c: T) = Equality(left, Const(c))

    def =:=[K <: RTCol[_, T]](right: K) = Equality(left, right)
    def <[K <: RTCol[_, T]](right: K)   = Less(left, right)
    def >[K <: RTCol[_, T]](right: K)   = Greater(left, right)
    def <=[K <: RTCol[_, T]](right: K)  = LessOrEq(left, right)
    def >=[K <: RTCol[_, T]](right: K)  = GreaterOrEq(left, right)

    def some = left.map(x => Some(x): Option[T])
  }

  implicit class RTColSyntax[T: TypeTag](right: RTCol[_, T]) extends Serializable {
    def <(c: T)   = Less(right, Const(c))
    def >(c: T)   = Greater(right, Const(c))
    def <=(c: T)  = LessOrEq(right, Const(c))
    def >=(c: T)  = GreaterOrEq(right, Const(c))
    def =:=(c: T) = Equality(right, Const(c))

    def some = right.map(x => Some(x): Option[T])
  }

  implicit class ExpressionSyntax(expr: Expression) {
    def &&(other: Expression) = And(expr, other)
  }
}
