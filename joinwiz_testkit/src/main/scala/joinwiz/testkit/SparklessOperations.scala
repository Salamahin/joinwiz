package joinwiz.testkit

import joinwiz.dataset._
import joinwiz.syntax.JOIN_CONDITION
import joinwiz.{ApplyLeft, ApplyRight, ComputationEngine}

import scala.reflect.runtime.universe.TypeTag

object SparklessOperations extends ComputationEngine[Seq] {

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
    override def apply[K: TypeTag](ft: Seq[T])(func: T => K): GrouppedByKeySyntax[Seq, T, K] =
      new GrouppedByKeySyntax[Seq, T, K] {
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
      }
  }

  override def unionByName[T]: UnionByName[Seq, T] = new UnionByName[Seq, T] {
    override def apply(ft1: Seq[T])(ft2: Seq[T]): Seq[T] = ft1 ++ ft2
  }

  override def collect[T]: Collect[Seq, T] = new Collect[Seq, T] {
    override def apply(ft: Seq[T]): Seq[T] = ft
  }
}
