package joinwiz

import joinwiz.api.{Collect, Distinct, Filter, FlatMap, GroupByKey, Join, KeyValueGroupped, Map, UnionByName, WithWindow}
import joinwiz.syntax.{JOIN_CONDITION, WINDOW_EXPRESSION}
import joinwiz.window.TWindowSpec
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.{Dataset, Encoder}

import scala.reflect.runtime.universe.TypeTag

package object spark {

  implicit val sparkBasedEngine: ComputationEngine[Dataset] = new ComputationEngine[Dataset] {
    override def join: Join[Dataset] = new Join[Dataset] {

      def joinWiz[L, R](fl: Dataset[L], fr: Dataset[R], joinType: String)(joinBy: JOIN_CONDITION[L, R]): Dataset[(L, R)] = {
        fl.as(joinwiz.alias.left)
          .joinWith(
            fr.as(joinwiz.alias.right),
            joinBy(TColumn.left, TColumn.right)(),
            joinType
          )
      }

      override def inner[L, R](fl: Dataset[L], fr: Dataset[R])(expr: JOIN_CONDITION[L, R]): Dataset[(L, R)] = {
        joinWiz(fl, fr, "inner")(expr)
      }

      override def left[L: TypeTag, R: TypeTag](fl: Dataset[L], fr: Dataset[R])(expr: JOIN_CONDITION[L, R]): Dataset[(L, Option[R])] = {
        implicit val enc: Encoder[(L, Option[R])] = ExpressionEncoder[(L, Option[R])]()

        joinWiz(fl, fr, "left_outer")(expr)
          .map {
            case (x, y) => (x, Option(y))
          }
      }

      override def left_anti[L: TypeTag, R](fl: Dataset[L], fr: Dataset[R])(expr: JOIN_CONDITION[L, R]): Dataset[L] = {
        implicit val enc: Encoder[L] = ExpressionEncoder[L]()

        fl.as(joinwiz.alias.left)
          .join(
            fr.as(joinwiz.alias.right),
            expr(TColumn.left, TColumn.right)(),
            "left_anti"
          )
          .as[L]
      }
    }

    override def map: Map[Dataset] = new Map[Dataset] {
      override def apply[T, U: TypeTag](ft: Dataset[T])(func: T => U): Dataset[U] =
        ft.map(func)(ExpressionEncoder())
    }

    override def flatMap: FlatMap[Dataset] = new FlatMap[Dataset] {
      override def apply[L, R: TypeTag](ft: Dataset[L])(func: L => Iterable[R]): Dataset[R] =
        ft.flatMap(func)(ExpressionEncoder())
    }

    override def filter: Filter[Dataset] = new Filter[Dataset] {
      override def apply[L](ft: Dataset[L])(predicate: L => Boolean): Dataset[L] =
        ft.filter(predicate)
    }

    override def distinct: Distinct[Dataset] = new Distinct[Dataset] {
      override def apply[T](ft: Dataset[T]): Dataset[T] = ft.distinct()
    }

    override def groupByKey: GroupByKey[Dataset] = new GroupByKey[Dataset] {
      override def apply[T, K: TypeTag](ft: Dataset[T])(func: T => K): KeyValueGroupped[Dataset, T, K] =
        new KeyValueGroupped[Dataset, T, K] {
          override def mapGroups[U: TypeTag](f: (K, Iterator[T]) => U): Dataset[U] =
            ft.groupByKey(func)(ExpressionEncoder()).mapGroups(f)(ExpressionEncoder())

          override def reduceGroups(f: (T, T) => T): Dataset[(K, T)] =
            ft.groupByKey(func)(ExpressionEncoder()).reduceGroups(f)

          override def count(): Dataset[(K, Long)] = ft.groupByKey(func)(ExpressionEncoder()).count()

          override def cogroup[U, R: TypeTag](
            other: KeyValueGroupped[Dataset, U, K]
          )(f: (K, Iterator[T], Iterator[U]) => Iterable[R]): Dataset[R] = {
            val otherDs = other.underlying.groupByKey(other.keyFunc)(ExpressionEncoder[K]())

            ft.groupByKey(func)(ExpressionEncoder[K]())
              .cogroup(otherDs)(f)(ExpressionEncoder[R]())
          }

          override def underlying: Dataset[T] = ft

          override def keyFunc: T => K = func
        }
    }

    override def unionByName: UnionByName[Dataset] =
      new UnionByName[Dataset] {
        override def apply[T](ft1: Dataset[T])(ft2: Dataset[T]): Dataset[T] = ft1 unionByName ft2
      }

    override def collect: Collect[Dataset] = new Collect[Dataset] {
      override def apply[T](ft: Dataset[T]): Seq[T] = ft.collect().toSeq
    }

    override def withWindow: WithWindow[Dataset] = new WithWindow[Dataset] {
      override def apply[T: TypeTag, S: TypeTag](fo: Dataset[T])(withWindow: WINDOW_EXPRESSION[T, S]): Dataset[(T, S)] = {
        val TWindowSpec(func, window) = withWindow(new ApplyTWindow[T])

        fo.withColumn("joinwiz_window", func(window()))
          .select(
            struct(fo.columns.map(col).toSeq: _*),
            col("joinwiz_window")
          )
          .as[(T, S)](ExpressionEncoder[(T, S)]())
      }
    }
  }
}
