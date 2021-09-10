package joinwiz

import joinwiz.api.{Collect, Distinct, Filter, FlatMap, GroupByKey, Join, KeyValueGroupped, Map, UnionByName, WithWindow}
import joinwiz.syntax.{JOIN_CONDITION, WINDOW_EXPRESSION}
import joinwiz.window.TWindowSpec

import scala.reflect.runtime.universe.TypeTag
import scala.reflect.runtime.universe

package object testkit {

  implicit val fakeComputationEngine: ComputationEngine[Seq] = new ComputationEngine[Seq] {
    override def join: Join[Seq] = new Join[Seq] {
      override def inner[T, U](ft: Seq[T], fu: Seq[U])(expr: JOIN_CONDITION[T, U]): Seq[(T, U)] = {
        new SeqJoinImpl[T, U](expr(ApplyLTCol[T, U], ApplyRTCol[T, U]), ft, fu).innerJoin()
      }

      override def left[T: TypeTag, U: TypeTag](ft: Seq[T], fu: Seq[U])(expr: JOIN_CONDITION[T, U]): Seq[(T, Option[U])] = {
        new SeqJoinImpl[T, U](expr(ApplyLTCol[T, U], ApplyRTCol[T, U]), ft, fu).leftJoin()
      }

      override def left_anti[T: TypeTag, U](ft: Seq[T], fu: Seq[U])(expr: JOIN_CONDITION[T, U]): Seq[T] = {
        new SeqJoinImpl[T, U](expr(ApplyLTCol[T, U], ApplyRTCol[T, U]), ft, fu).leftAntiJoin()
      }
    }

    override def map: Map[Seq] = new Map[Seq] {
      override def apply[T, U: TypeTag](ft: Seq[T])(func: T => U): Seq[U] = ft.map(func)
    }

    override def flatMap: FlatMap[Seq] = new FlatMap[Seq] {
      override def apply[T, U: TypeTag](ft: Seq[T])(func: T => TraversableOnce[U]): Seq[U] =
        ft.flatMap(func)
    }

    override def filter: Filter[Seq] = new Filter[Seq] {
      override def apply[T](ft: Seq[T])(predicate: T => Boolean): Seq[T] =
        ft.filter(predicate)
    }

    override def distinct: Distinct[Seq] = new Distinct[Seq] {
      override def apply[T](ft: Seq[T]): Seq[T] = ft.distinct
    }

    override def groupByKey: GroupByKey[Seq] = new GroupByKey[Seq] {
      override def apply[T, K: TypeTag](ft: Seq[T])(func: T => K): KeyValueGroupped[Seq, T, K] =
        new KeyValueGroupped[Seq, T, K] {
          override def mapGroups[U: TypeTag](f: (K, Iterator[T]) => U): Seq[U] =
            ft.groupBy(func)
              .map {
                case (k, vals) => f(k, vals.iterator)
              }
              .toSeq

          override def reduceGroups(f: (T, T) => T): Seq[(K, T)] =
            ft.groupBy(func)
              .mapValues(_.reduce(f))
              .toSeq

          override def underlying: Seq[T] = ft

          override def keyFunc: T => K = func

          override def count(): Seq[(K, Long)] = ft.groupBy(func).mapValues(_.size: Long).toSeq

          override def cogroup[U, R: universe.TypeTag](other: KeyValueGroupped[Seq, U, K])(
            f: (K, Iterator[T], Iterator[U]) => TraversableOnce[R]
          ): Seq[R] = {
            val ftGroupped    = ft.groupBy(func)
            val otherGroupped = other.underlying.groupBy(other.keyFunc)

            val keys = ftGroupped.keySet ++ otherGroupped.keySet

            for {
              key    <- keys.toList
              left   = ftGroupped.getOrElse(key, Nil)
              right  = otherGroupped.getOrElse(key, Nil)
              mapped <- f(key, left.iterator, right.iterator)
            } yield mapped
          }
        }
    }

    override def unionByName: UnionByName[Seq] = new UnionByName[Seq] {
      override def apply[T](ft1: Seq[T])(ft2: Seq[T]): Seq[T] = ft1 ++ ft2
    }

    override def collect: Collect[Seq] = new Collect[Seq] {
      override def apply[T](ft: Seq[T]): Seq[T] = ft
    }

    override def withWindow: WithWindow[Seq] = new WithWindow[Seq] {
      override def apply[T: TypeTag, S: TypeTag](fo: Seq[T])(withWindow: WINDOW_EXPRESSION[T, S]): Seq[(T, S)] = {
        val TWindowSpec(func, window) = withWindow(new ApplyTWindow[T])

        fo.groupBy(window.apply)
          .values
          .map(vals => window.ordering.map(implicit ord => vals.sorted).getOrElse(vals))
          .flatMap(vals => vals zip func(vals))
          .toSeq
      }
    }
  }
}
