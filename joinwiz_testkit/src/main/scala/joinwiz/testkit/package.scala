package joinwiz
import joinwiz.dataset.{Collect, Distinct, Filter, FlatMap, GroupByKey, Join, KeyValueGroupped, Map, UnionByName}
import joinwiz.syntax.JOIN_CONDITION

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.TypeTag

package object testkit {
  implicit val fakeComputationEngine = new ComputationEngine[Seq] {
    override def join[T]: Join[Seq, T] = new Join[Seq, T] {
      override def inner[U](ft: Seq[T], fu: Seq[U])(expr: JOIN_CONDITION[T, U]): Seq[(T, U)] = {
        new SeqJoinImpl[T, U](expr(ApplyLeft[T], ApplyRight[U]), ft, fu).innerJoin()
      }

      override def left[U](ft: Seq[T], fu: Seq[U])(expr: JOIN_CONDITION[T, U]): Seq[(T, U)] = {
        new SeqJoinImpl[T, U](expr(ApplyLeft[T], ApplyRight[U]), ft, fu).leftJoin()
      }
    }

    override def map[T]: Map[Seq, T] = new Map[Seq, T] {
      override def apply[U: TypeTag](ft: Seq[T])(func: T => U): Seq[U] = ft.map(func)
    }

    override def flatMap[T]: FlatMap[Seq, T] = new FlatMap[Seq, T] {
      override def apply[U: TypeTag](ft: Seq[T])(func: T => TraversableOnce[U]): Seq[U] =
        ft.flatMap(func)
    }

    override def filter[T]: Filter[Seq, T] = new Filter[Seq, T] {
      override def apply(ft: Seq[T])(predicate: T => Boolean): Seq[T] =
        ft.filter(predicate)
    }

    override def distinct[T]: Distinct[Seq, T] = new Distinct[Seq, T] {
      override def apply(ft: Seq[T]): Seq[T] = ft.distinct
    }

    override def groupByKey[T]: GroupByKey[Seq, T] = new GroupByKey[Seq, T] {
      override def apply[K: TypeTag](ft: Seq[T])(func: T => K): KeyValueGroupped[Seq, T, K] =
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

    override def unionByName[T]: UnionByName[Seq, T] = new UnionByName[Seq, T] {
      override def apply(ft1: Seq[T])(ft2: Seq[T]): Seq[T] = ft1 ++ ft2
    }

    override def collect[T]: Collect[Seq, T] = new Collect[Seq, T] {
      override def apply(ft: Seq[T]): Seq[T] = ft
    }
  }
}
