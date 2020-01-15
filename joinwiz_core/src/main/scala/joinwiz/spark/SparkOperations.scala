package joinwiz.spark

import joinwiz.ops._
import joinwiz.syntax.JOIN_CONDITION
import joinwiz.{DatasetOperations, LTColumnExtractor, RTColumnExtractor}
import org.apache.spark.sql.{Dataset, Encoders}

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.TypeTag

object SparkOperations extends DatasetOperations[Dataset] {
  val LEFT_DS_ALIAS = "left"
  val RIGHT_DS_ALIAS = "right"

  override def join[L]: Join[Dataset, L] = new Join[Dataset, L] {

    def joinWiz[R](fl: Dataset[L], fr: Dataset[R], joinType: String)(joinBy: JOIN_CONDITION[L, R]): Dataset[(L, R)] = {
      fl
        .as(LEFT_DS_ALIAS)
        .joinWith(
          fr.as(RIGHT_DS_ALIAS),
          new ColumnEvaluator().evaluate(joinBy(LTColumnExtractor[L], RTColumnExtractor[R])),
          joinType
        )
    }

    override def inner[R](fl: Dataset[L], fr: Dataset[R])(expr: JOIN_CONDITION[L, R]): Dataset[(L, R)] = {
      joinWiz(fl, fr, "inner")(expr)
    }

    override def left[R](fl: Dataset[L], fr: Dataset[R])(expr: JOIN_CONDITION[L, R]): Dataset[(L, R)] = {
      joinWiz(fl, fr, "left_outer")(expr)
    }
  }

  override def map[T]: Map[Dataset, T] = new Map[Dataset, T] {
    override def apply[U <: Product : universe.TypeTag](ft: Dataset[T])(func: T => U): Dataset[U] =
      ft.map(func)(Encoders.product)
  }

  override def flatMap[L]: FlatMap[Dataset, L] = new FlatMap[Dataset, L] {
    override def apply[R <: Product : TypeTag](ft: Dataset[L])(func: L => TraversableOnce[R]): Dataset[R] =
      ft.flatMap(func)(Encoders.product)
  }

  override def filter[L]: Filter[Dataset, L] = new Filter[Dataset, L] {
    override def apply(ft: Dataset[L])(predicate: L => Boolean): Dataset[L] =
      ft.filter(predicate)
  }
}
