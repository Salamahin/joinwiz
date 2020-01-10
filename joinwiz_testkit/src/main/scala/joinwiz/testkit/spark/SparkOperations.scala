package joinwiz.testkit.spark

import joinwiz.syntax.JOIN_CONDITION
import joinwiz.testkit.ops._
import joinwiz.testkit.DatasetOperations
import org.apache.spark.sql.{Dataset, Encoders}

import scala.reflect.runtime.universe.TypeTag

object SparkOperations extends DatasetOperations[Dataset] {
  override def join[T]: Join[Dataset, T] = new Join[Dataset, T] {
    override def inner[U](ft: Dataset[T], fu: Dataset[U])(expr: JOIN_CONDITION[T, U]): Dataset[(T, U)] = {
      import joinwiz.syntax._
      ft.innerJoin(fu)(expr)
    }

    override def left[U](ft: Dataset[T], fu: Dataset[U])(expr: JOIN_CONDITION[T, U]): Dataset[(T, U)] = {
      import joinwiz.syntax._
      ft.leftJoin(fu)(expr)
    }
  }

  override def map[T]: Map[Dataset, T] = new Map[Dataset, T] {
    def apply[U <: Product : TypeTag](ft: Dataset[T])(func: T => U): Dataset[U] =
      ft.map(func)(Encoders.product[U])
  }

  override def flatMap[T]: FlatMap[Dataset, T] = new FlatMap[Dataset, T] {
    override def apply[U <: Product : TypeTag](ft: Dataset[T])(func: T => TraversableOnce[U]): Dataset[U] =
      ft.flatMap(func)(Encoders.product[U])
  }

  override def filter[T]: Filter[Dataset, T] = new Filter[Dataset, T] {
    override def apply(ft: Dataset[T])(predicate: T => Boolean): Dataset[T] =
      ft.filter(predicate)
  }
}
