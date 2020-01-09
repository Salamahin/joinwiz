package joinwiz.spark

import joinwiz.JoinWiz.JOIN_CONDITION
import joinwiz.ops.Join
import joinwiz.{DSLike, JoinWiz, ops}
import org.apache.spark.sql.{Dataset, Encoders}

object SparkDS extends DSLike[Dataset] {
  override implicit def joinInstance[T]: Join[Dataset, T] = new Join[Dataset, T] {
    override def innerJoin[U](ft: Dataset[T], fu: Dataset[U])(expr: JOIN_CONDITION[T, U]): Dataset[(T, U)] = {
      import JoinWiz._
      ft.innerJoin(fu)(expr)
    }

    override def leftJoin[U](ft: Dataset[T], fu: Dataset[U])(expr: JOIN_CONDITION[T, U]): Dataset[(T, U)] = {
      import JoinWiz._
      ft.leftJoin(fu)(expr)
    }
  }

  override implicit def mapInstance[T]: ops.Map[Dataset, T] = new ops.Map[Dataset, T] {
    def map[U <: Product : reflect.runtime.universe.TypeTag](ft: Dataset[T])(func: T => U): Dataset[U] = {
      ft.map(func)(Encoders.product[U])
    }
  }
}
