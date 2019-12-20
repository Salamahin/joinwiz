package joinwiz

import joinwiz.JoinWiz.JOIN_CONDITION
import org.apache.spark.sql.{Dataset, Encoder}

import scala.language.higherKinds

trait JoinApi[F[_], T] {
  def innerJoin[U](ft: F[T], fu: F[U])(expr: JOIN_CONDITION[T, U]): F[(T, U)]

  def leftJoin[U](ft: F[T], fu: F[U])(expr: JOIN_CONDITION[T, U]): F[(T, U)]
}

trait MapApi[F[_], E[_], T] {
  def map[U: E](ft: F[T])(func: T => U): F[U]
}

object SparklessApi {

  case class FakeEncoder[T]()

  implicit def fakeEncoder[T]: FakeEncoder[T] = FakeEncoder[T]()

  implicit def joinInstance[T]: JoinApi[Seq, T] = new JoinApi[Seq, T] {
    override def innerJoin[U](ft: Seq[T], fu: Seq[U])(expr: JOIN_CONDITION[T, U]): Seq[(T, U)] = {
      new SeqJoinOperator[T, U](expr(new LTColumnExtractor[T], new RTColumnExtractor[U]), ft, fu).innerJoin()
    }

    override def leftJoin[U](ft: Seq[T], fu: Seq[U])(expr: JOIN_CONDITION[T, U]): Seq[(T, U)] = {
      new SeqJoinOperator[T, U](expr(new LTColumnExtractor[T], new RTColumnExtractor[U]), ft, fu).leftJoin()
    }
  }

  implicit def mapInstance[T]: MapApi[Seq, FakeEncoder, T] = new MapApi[Seq, FakeEncoder, T] {
    override def map[U: FakeEncoder](ft: Seq[T])(func: T => U): Seq[U] = ft.map(func)
  }
}

object SparkApi {
  implicit def joinInstance[T: Encoder]: JoinApi[Dataset, T] = new JoinApi[Dataset, T] {
    override def innerJoin[U](ft: Dataset[T], fu: Dataset[U])(expr: JOIN_CONDITION[T, U]): Dataset[(T, U)] = {
      import JoinWiz._
      ft.innerJoin(fu)(expr)
    }

    override def leftJoin[U](ft: Dataset[T], fu: Dataset[U])(expr: JOIN_CONDITION[T, U]): Dataset[(T, U)] = {
      import JoinWiz._
      ft.leftJoin(fu)(expr)
    }
  }

  implicit def mapInstance[T: Encoder]: MapApi[Dataset, Encoder, T] = new MapApi[Dataset, Encoder, T] {
    override def map[U: Encoder](ft: Dataset[T])(func: T => U): Dataset[U] = ft.map(func)
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

    def map[U, V[_]](func: T => U)(implicit api: MapApi[F, V, T], enc: V[U]) = new DatasetApiSyntax(api.map(ft)(func))

    def get: F[T] = ft
  }

  def apply[F[_], T, U](ft: F[T])(func: DatasetApiSyntax[F, T] => DatasetApiSyntax[F, U]): F[U] = {
    func(new DatasetApiSyntax(ft)).get
  }
}