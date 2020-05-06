package joinwiz.testkit

import joinwiz.ops._
import joinwiz.syntax.JOIN_CONDITION
import joinwiz.{ApplyToLeftColumn, ApplyToRightColumn, DatasetOperations}

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.TypeTag

object SparklessOperations extends DatasetOperations[Seq] {

  override def join[T]: Join[Seq, T] = new Join[Seq, T] {
    override def inner[U](ft: Seq[T], fu: Seq[U])(expr: JOIN_CONDITION[T, U]): Seq[(T, U)] = {
      new SeqJoinImpl[T, U](expr(ApplyToLeftColumn[T], ApplyToRightColumn[U]), ft, fu).innerJoin()
    }

    override def left[U](ft: Seq[T], fu: Seq[U])(expr: JOIN_CONDITION[T, U]): Seq[(T, U)] = {
      new SeqJoinImpl[T, U](expr(ApplyToLeftColumn[T], ApplyToRightColumn[U]), ft, fu).leftJoin()
    }
  }

  override def map[T]: Map[Seq, T] = new Map[Seq, T] {
    def apply[U <: Product: reflect.runtime.universe.TypeTag](ft: Seq[T])(func: T => U): Seq[U] =
      ft.map(func)
  }

  override def flatMap[T]: FlatMap[Seq, T] = new FlatMap[Seq, T] {
    override def apply[U <: Product: universe.TypeTag](ft: Seq[T])(func: T => TraversableOnce[U]): Seq[U] =
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
    override def apply[K <: Product: TypeTag](ft: Seq[T])(func: T => K): GrouppedByKeySyntax[Seq, T, K] =
      new GrouppedByKeySyntax[Seq, T, K] {
        override def mapGroups[U <: Product: TypeTag](f: (K, Iterator[T]) => U): Seq[U] =
          ft.groupBy(func)
            .map {
              case (k, vals) => f(k, vals.iterator)
            }
            .toSeq
      }
  }
}
