package joinwiz.sparkless

import joinwiz.JoinWiz.JOIN_CONDITION
import joinwiz.ops.Join
import joinwiz.{DSLike, LTColumnExtractor, RTColumnExtractor, ops}

object SparklessDS extends DSLike[Seq] {

  override implicit def joinInstance[T]: Join[Seq, T] = new Join[Seq, T] {
    override def innerJoin[U](ft: Seq[T], fu: Seq[U])(expr: JOIN_CONDITION[T, U]): Seq[(T, U)] = {
      new SeqJoinOperator[T, U](expr(new LTColumnExtractor[T], new RTColumnExtractor[U]), ft, fu).innerJoin()
    }

    override def leftJoin[U](ft: Seq[T], fu: Seq[U])(expr: JOIN_CONDITION[T, U]): Seq[(T, U)] = {
      new SeqJoinOperator[T, U](expr(new LTColumnExtractor[T], new RTColumnExtractor[U]), ft, fu).leftJoin()
    }
  }

  override implicit def mapInstance[T]: ops.Map[Seq, T] = new ops.Map[Seq, T] {
    def map[U <: Product : reflect.runtime.universe.TypeTag](ft: Seq[T])(func: T => U): Seq[U] = ft.map(func)
  }
}
