package joinwiz

import org.apache.spark.sql.{Dataset, Encoder}

trait MapLike[F[_], T, U] {
  def map(ft: F[T])(func: T => U): F[U]
}

object MapLike {
  implicit def dsMapLike[T, U: Encoder]: MapLike[Dataset, T, U] = new MapLike[Dataset, T, U] {
    override def map(ft: Dataset[T])(func: T => U): Dataset[U] = {
      ft.map(func)
    }
  }

  implicit def seqMapLike[T, U]: MapLike[Seq, T, U] = new MapLike[Seq, T, U] {
    override def map(ft: Seq[T])(func: T => U): Seq[U] = ft.map(func)
  }
}
