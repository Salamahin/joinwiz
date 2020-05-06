package joinwiz

import joinwiz.ops.{Distinct, Filter, FlatMap, GroupByKey, Join, Map}

import scala.language.higherKinds

trait DatasetOperations[F[_]] {

  def join[T]: Join[F, T]

  def map[T]: Map[F, T]

  def flatMap[T]: FlatMap[F, T]

  def filter[T]: Filter[F, T]

  def distinct[T]: Distinct[F, T]

  def groupByKey[T]: GroupByKey[F, T]
}
