package joinwiz

import joinwiz.dataset.KeyValueGroupped
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.udf

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

sealed trait LowLevelTColSyntax {
  private[joinwiz] def mapLeft[O, S: TypeTag](c: Column)(f: O => S): LTCol[O, S] = new LTCol[O, S] {
    override def column: Column     = c
    override def apply(value: O): S = f(value)
  }

  private[joinwiz] def mapRight[O, S: TypeTag](c: Column)(f: O => S): RTCol[O, S] = new RTCol[O, S] {
    override def column: Column     = c
    override def apply(value: O): S = f(value)
  }

  implicit class MapLeftOptionColumnSyntax[O, T: TypeTag](lt: LTCol[O, T]) extends Serializable {
    def map[S: TypeTag](f: T => S): LTCol[O, S] =
      mapLeft(udf((x: T) => f(x)) apply lt.column)(value => (f compose lt.apply)(value))

    def some: LTCol[O, Option[T]] = mapLeft(lt.column)(value => Some(lt(value)))
  }

  implicit class MapRightOptionColumnSyntax[O, T: TypeTag](rt: RTCol[O, T]) extends Serializable {
    def map[S: TypeTag](f: T => S): RTCol[O, S] =
      mapRight(udf((x: T) => f(x)) apply rt.column)(value => (f compose rt.apply)(value))

    def some: RTCol[O, Option[T]] = mapRight(rt.column)(value => Some(rt(value)))
  }
}

object syntax extends LowLevelTColSyntax {
  type JOIN_CONDITION[L, R] = (ApplyLeft[L, L], ApplyRight[R, R]) => Expression

  implicit class MapRightOptionColumnSyntax[O, T: TypeTag](rt: RTCol[O, Option[T]]) extends Serializable {
    def map[S: TypeTag](f: T => S): RTCol[O, Option[S]] =
      mapRight(udf((x: T) => f(x)) apply rt.column)(value => rt.apply(value).map(f))
  }

  implicit class MapLeftOptionColumnSyntax[O, T: TypeTag](lt: LTCol[O, Option[T]]) extends Serializable {
    def map[S: TypeTag](f: T => S): LTCol[O, Option[S]] =
      mapLeft(udf((x: T) => f(x)) apply lt.column)(value => lt.apply(value).map(f))
  }

  implicit class DatasetLikeSyntax[F[_], T: ClassTag](ft: F[T])(implicit ce: ComputationEngine[F]) {
    def innerJoin[U](fu: F[U])(expr: JOIN_CONDITION[T, U]): F[(T, U)] =
      ce.join.inner(ft, fu)(expr)

    def leftJoin[U](fu: F[U])(expr: JOIN_CONDITION[T, U]): F[(T, U)] =
      ce.join.left(ft, fu)(expr)

    def map[U: TypeTag](func: T => U): F[U] =
      ce.map(ft)(func)

    def flatMap[U: TypeTag](func: T => Seq[U]): F[U] =
      ce.flatMap(ft)(func)

    def filter(func: T => Boolean): F[T] =
      ce.filter(ft)(func)

    def distinct(): F[T] =ce.distinct(ft)

    def groupByKey[K: TypeTag](func: T => K): KeyValueGroupped[F, T, K] =
      ce.groupByKey(ft)(func)

    def unionByName(other: F[T]): F[T] =
      ce.unionByName(ft)(other)

    def collect(): Seq[T] = ce.collect(ft)
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
  }

  implicit class RTColSyntax[T: TypeTag](right: RTCol[_, T]) extends Serializable {
    def <(c: T)   = Less(right, Const(c))
    def >(c: T)   = Greater(right, Const(c))
    def <=(c: T)  = LessOrEq(right, Const(c))
    def >=(c: T)  = GreaterOrEq(right, Const(c))
    def =:=(c: T) = Equality(right, Const(c))
  }

  implicit class ExpressionSyntax(expr: Expression) {
    def &&(other: Expression) = And(expr, other)
  }
}
