package joinwiz

import joinwiz.ops.{Filter, FlatMap, Join, Map}

import scala.language.higherKinds

trait DatasetOperations[F[_]] {

  def join[T]: Join[F, T]

  def map[T]: Map[F, T]

  def flatMap[T]: FlatMap[F, T]

  def filter[T]: Filter[F, T]

}