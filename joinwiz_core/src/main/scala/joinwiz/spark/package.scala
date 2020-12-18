package joinwiz

import joinwiz.api.{Collect, Distinct, Filter, FlatMap, GroupByKey, Join, KeyValueGroupped, Map, UnionByName}
import joinwiz.syntax.JOIN_CONDITION
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Dataset, Encoder}

import scala.reflect.runtime.universe.TypeTag

package object spark {

  implicit val sparkBasedEngine: ComputationEngine[Dataset] = new ComputationEngine[Dataset] {
    override def join[L]: Join[Dataset, L] = new Join[Dataset, L] {

      def joinWiz[R](fl: Dataset[L], fr: Dataset[R], joinType: String)(
        joinBy: JOIN_CONDITION[L, R]
      ): Dataset[(L, R)] = {
        fl.as(joinwiz.Left.alias)
          .joinWith(
            fr.as(joinwiz.Right.alias),
            joinBy(ApplyLTCol[L, R], ApplyRTCol[L, R])(),
            joinType
          )
      }

      override def inner[R](fl: Dataset[L], fr: Dataset[R])(expr: JOIN_CONDITION[L, R]): Dataset[(L, R)] = {
        joinWiz(fl, fr, "inner")(expr)
      }

      override def left[R](fl: Dataset[L], fr: Dataset[R])(expr: JOIN_CONDITION[L, R])(implicit tt: TypeTag[(L, Option[R])]): Dataset[(L, Option[R])] = {
        implicit val enc: Encoder[(L, Option[R])] = ExpressionEncoder[(L, Option[R])]()

        joinWiz(fl, fr, "left_outer")(expr)
          .map {
            case (x, y) => (x, Option(y))
          }
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
      override def apply[K: TypeTag](ft: Dataset[T])(func: T => K): KeyValueGroupped[Dataset, T, K] =
        new KeyValueGroupped[Dataset, T, K] {
          override def mapGroups[U: TypeTag](f: (K, Iterator[T]) => U): Dataset[U] =
            ft.groupByKey(func)(ExpressionEncoder()).mapGroups(f)(ExpressionEncoder())

          override def reduceGroups(f: (T, T) => T): Dataset[(K, T)] =
            ft.groupByKey(func)(ExpressionEncoder()).reduceGroups(f)

          override def count(): Dataset[(K, Long)] = ft.groupByKey(func)(ExpressionEncoder()).count()

          override def cogroup[U, R: TypeTag](
            other: KeyValueGroupped[Dataset, U, K]
          )(f: (K, Iterator[T], Iterator[U]) => TraversableOnce[R]): Dataset[R] = {
            val otherDs = other.underlying.groupByKey(other.keyFunc)(ExpressionEncoder())

            ft.groupByKey(func)(ExpressionEncoder())
              .cogroup(otherDs)(f)(ExpressionEncoder[R]())
          }

          override def underlying: Dataset[T] = ft

          override def keyFunc: T => K = func
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
