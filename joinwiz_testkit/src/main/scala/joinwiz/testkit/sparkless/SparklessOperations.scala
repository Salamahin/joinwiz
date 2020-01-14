package joinwiz.testkit.sparkless

import joinwiz.syntax.JOIN_CONDITION
import joinwiz.testkit.DatasetOperations
import joinwiz.testkit.ops._
import joinwiz.{LTColumnExtractor, RTColumnExtractor}

import scala.reflect.runtime.universe

object SparklessOperations extends DatasetOperations[Seq] {

  override def join[T]: Join[Seq, T] = new Join[Seq, T] {
    override def inner[U](ft: Seq[T], fu: Seq[U])(expr: JOIN_CONDITION[T, U]): Seq[(T, U)] = {
      new SeqJoinImpl[T, U](expr(new LTColumnExtractor[T, T](extractor = identity), new RTColumnExtractor[U]), ft, fu).innerJoin()
    }

    override def left[U](ft: Seq[T], fu: Seq[U])(expr: JOIN_CONDITION[T, U]): Seq[(T, U)] = {
      new SeqJoinImpl[T, U](expr(new LTColumnExtractor[T, T](extractor = identity), new RTColumnExtractor[U]), ft, fu).leftJoin()
    }
  }

  override def map[T]: Map[Seq, T] = new Map[Seq, T] {
    def apply[U <: Product : reflect.runtime.universe.TypeTag](ft: Seq[T])(func: T => U): Seq[U] =
      ft.map(func)
  }

  override def flatMap[T]: FlatMap[Seq, T] = new FlatMap[Seq, T] {
    override def apply[U <: Product : universe.TypeTag](ft: Seq[T])(func: T => TraversableOnce[U]): Seq[U] =
      ft.flatMap(func)
  }

  override def filter[T]: Filter[Seq, T] = new Filter[Seq, T] {
    override def apply(ft: Seq[T])(predicate: T => Boolean): Seq[T] =
      ft.filter(predicate)
  }
}
