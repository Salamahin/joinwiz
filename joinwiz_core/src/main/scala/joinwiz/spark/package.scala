package joinwiz

import joinwiz.dataset.{Collect, Distinct, Filter, FlatMap, GroupByKey, GrouppedByKeySyntax, Join, Map, UnionByName}
import joinwiz.syntax.JOIN_CONDITION
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import scala.reflect.runtime.universe.TypeTag

package object spark {

  implicit val sparkBasedEngine = new ComputationEngine[Dataset] {
    override def join[L]: Join[Dataset, L] = new Join[Dataset, L] {

      def joinWiz[R](fl: Dataset[L], fr: Dataset[R], joinType: String)(
        joinBy: JOIN_CONDITION[L, R]
      ): Dataset[(L, R)] = {
        fl.as(joinwiz.Left.alias)
          .joinWith(
            fr.as(joinwiz.Right.alias),
            SparkExpressionEvaluator.evaluate(joinBy(ApplyLeft[L], ApplyRight[R])),
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
      override def apply[U: TypeTag](ft: Dataset[T])(func: T => U): Dataset[U] =
        ft.map(func)(ExpressionEncoder())
    }

    override def flatMap[L]: FlatMap[Dataset, L] = new FlatMap[Dataset, L] {
      override def apply[R: TypeTag](ft: Dataset[L])(func: L => TraversableOnce[R]): Dataset[R] =
        ft.flatMap(func)(ExpressionEncoder())
    }

    override def filter[L]: Filter[Dataset, L] = new Filter[Dataset, L] {
      override def apply(ft: Dataset[L])(predicate: L => Boolean): Dataset[L] =
        ft.filter(predicate)
    }

    override def distinct[T]: Distinct[Dataset, T] = new Distinct[Dataset, T] {
      override def apply(ft: Dataset[T]): Dataset[T] = ft.distinct()
    }

    override def groupByKey[T]: GroupByKey[Dataset, T] = new GroupByKey[Dataset, T] {
      override def apply[K: TypeTag](ft: Dataset[T])(func: T => K): GrouppedByKeySyntax[Dataset, T, K] =
        new GrouppedByKeySyntax[Dataset, T, K] {
          override def mapGroups[U: TypeTag](f: (K, Iterator[T]) => U): Dataset[U] =
            ft.groupByKey(func)(ExpressionEncoder()).mapGroups(f)(ExpressionEncoder())

          override def reduceGroups(f: (T, T) => T): Dataset[(K, T)] =
            ft.groupByKey(func)(ExpressionEncoder()).reduceGroups(f)
        }
    }

    override def unionByName[T]: UnionByName[Dataset, T] =
      new UnionByName[Dataset, T] {
        override def apply(ft1: Dataset[T])(ft2: Dataset[T]): Dataset[T] = ft1 unionByName ft2
      }

    override def collect[T]: Collect[Dataset, T] = new Collect[Dataset, T] {
      override def apply(ft: Dataset[T]): Seq[T] = ft.collect()
    }
  }
}
