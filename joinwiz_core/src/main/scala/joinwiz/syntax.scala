package joinwiz

import joinwiz.api.KeyValueGroupped
import joinwiz.expression._
import joinwiz.window.{CommonWindowFunctions, TWindowSpec, WindowExpressionSyntax}

import scala.reflect.runtime.universe.TypeTag

object syntax
    extends EqualSyntax
    with CompareSyntax
    with CombinatorsSyntax
    with MapSyntax
    with IsEmptySyntax
    with Wrappers
    with UnapplySyntax
    with ExtractTColSyntax
    with CommonWindowFunctions
    with WindowExpressionSyntax {

  type JOIN_CONDITION[L, R]    = (ApplyLTCol[L, R, L], ApplyRTCol[L, R, R]) => Expr[L, R]
  type WINDOW_EXPRESSION[T, S] = ApplyTWindow[T] => TWindowSpec[T, S]

  implicit class DatasetLikeSyntax[F[_], T: TypeTag](ft: F[T])(implicit ce: ComputationEngine[F]) {
    def innerJoin[U](fu: F[U])(expr: JOIN_CONDITION[T, U]): F[(T, U)] =
      ce.join.inner(ft, fu)(expr)

    def leftJoin[U: TypeTag](fu: F[U])(expr: JOIN_CONDITION[T, U]): F[(T, Option[U])] =
      ce.join.left[T, U](ft, fu)(expr)

    def leftAntiJoin[U: TypeTag](fu: F[U])(expr: JOIN_CONDITION[T, U]): F[T] =
      ce.join.left_anti[T, U](ft, fu)(expr)

    def map[U: TypeTag](func: T => U): F[U] =
      ce.map(ft)(func)

    def flatMap[U: TypeTag](func: T => Seq[U]): F[U] =
      ce.flatMap(ft)(func)

    def filter(func: T => Boolean): F[T] =
      ce.filter(ft)(func)

    def distinct(): F[T] = ce.distinct(ft)

    def groupByKey[K: TypeTag](func: T => K): KeyValueGroupped[F, T, K] =
      ce.groupByKey(ft)(func)

    def unionByName(other: F[T]): F[T] =
      ce.unionByName(ft)(other)

    def collect(): Seq[T] = ce.collect(ft)

    def withWindow[S: TypeTag](expr: WINDOW_EXPRESSION[T, S]): F[(T, S)] =
      ce.withWindow(ft)(expr)
  }
}
