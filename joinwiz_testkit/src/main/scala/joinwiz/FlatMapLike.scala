package joinwiz

import org.apache.spark.sql.{Dataset, Encoder}

trait FlatMapLike[F[_], T, U] {
  def flatMap(ft: F[T])(func: T => TraversableOnce[U]): F[U]
}

object FlatMapLike {
  implicit def dsFlatMapLike[T, U: Encoder]: FlatMapLike[Dataset, T, U] = new FlatMapLike[Dataset, T, U] {
    override def flatMap(ft: Dataset[T])(func: T => TraversableOnce[U]): Dataset[U] = ft.flatMap(func)
  }

  implicit def seqFlatMapLike[T, U]: FlatMapLike[Seq, T, U] = new FlatMapLike[Seq, T, U] {
    override def flatMap(ft: Seq[T])(func: T => TraversableOnce[U]): Seq[U] = ft.flatMap(func)
  }
}
