package joinwiz

import joinwiz.JoinWiz.JOIN_CONDITION
import org.apache.spark.sql.{Dataset, Encoders}

import scala.language.higherKinds

trait JoinApi[F[_], T] {
  def innerJoin[U](ft: F[T], fu: F[U])(expr: JOIN_CONDITION[T, U]): F[(T, U)]

  def leftJoin[U](ft: F[T], fu: F[U])(expr: JOIN_CONDITION[T, U]): F[(T, U)]
}

trait MapApi[F[_], T] {
  def map[U <: Product : reflect.runtime.universe.TypeTag](ft: F[T])(func: T => U): F[U]
}

trait DatasetLikeImpl[F[_]] {

  implicit def joinInstance[T]: JoinApi[F, T]

  implicit def mapInstance[T]: MapApi[F, T]
}


object SparklessDatasetLike extends DatasetLikeImpl[Seq] {

  override implicit def joinInstance[T]: JoinApi[Seq, T] = new JoinApi[Seq, T] {
    override def innerJoin[U](ft: Seq[T], fu: Seq[U])(expr: JOIN_CONDITION[T, U]): Seq[(T, U)] = {
      new SeqJoinOperator[T, U](expr(new LTColumnExtractor[T], new RTColumnExtractor[U]), ft, fu).innerJoin()
    }

    override def leftJoin[U](ft: Seq[T], fu: Seq[U])(expr: JOIN_CONDITION[T, U]): Seq[(T, U)] = {
      new SeqJoinOperator[T, U](expr(new LTColumnExtractor[T], new RTColumnExtractor[U]), ft, fu).leftJoin()
    }
  }

  override implicit def mapInstance[T]: MapApi[Seq, T] = new MapApi[Seq, T] {
    def map[U <: Product : reflect.runtime.universe.TypeTag](ft: Seq[T])(func: T => U): Seq[U] = ft.map(func)
  }
}

object SparkDatasetLike extends DatasetLikeImpl[Dataset] {
  override implicit def joinInstance[T]: JoinApi[Dataset, T] = new JoinApi[Dataset, T] {
    override def innerJoin[U](ft: Dataset[T], fu: Dataset[U])(expr: JOIN_CONDITION[T, U]): Dataset[(T, U)] = {
      import JoinWiz._
      ft.innerJoin(fu)(expr)
    }

    override def leftJoin[U](ft: Dataset[T], fu: Dataset[U])(expr: JOIN_CONDITION[T, U]): Dataset[(T, U)] = {
      import JoinWiz._
      ft.leftJoin(fu)(expr)
    }
  }

  override implicit def mapInstance[T]: MapApi[Dataset, T] = new MapApi[Dataset, T] {
    def map[U <: Product : reflect.runtime.universe.TypeTag](ft: Dataset[T])(func: T => U): Dataset[U] = {
      ft.map(func)(Encoders.product[U])
    }
  }
}

object DatasetApi {

  class DatasetApiSyntax[F[_], T](ft: F[T]) {
    def innerJoin[U](fu: F[U])
                    (expr: JOIN_CONDITION[T, U])
                    (implicit api: JoinApi[F, T]) = new DatasetApiSyntax(api.innerJoin(ft, fu)(expr))

    def leftJoin[U](fu: F[U])
                   (expr: JOIN_CONDITION[T, U])
                   (implicit api: JoinApi[F, T]) = new DatasetApiSyntax(api.leftJoin(ft, fu)(expr))

    def map[U <: Product : reflect.runtime.universe.TypeTag](func: T => U)(implicit api: MapApi[F, T]) =
      new DatasetApiSyntax(api.map(ft)(func))

    def get: F[T] = ft
  }

  def apply[F[_], T, U](ft: F[T])(func: DatasetApiSyntax[F, T] => DatasetApiSyntax[F, U]): F[U] = {
    func(new DatasetApiSyntax(ft)).get
  }
}