package joinwiz

import joinwiz.dataset.{Collect, Distinct, Filter, FlatMap, GroupByKey, Join, Map, UnionByName}

import scala.reflect.ClassTag

trait ComputationEngine[F[_]] {

  def join[T]: Join[F, T]

  def map[T]: Map[F, T]

  def flatMap[T]: FlatMap[F, T]

  def filter[T]: Filter[F, T]

  def distinct[T]: Distinct[F, T]

  def groupByKey[T]: GroupByKey[F, T]

  def unionByName[T]: UnionByName[F, T]

  def collect[T]: Collect[F, T]
}
