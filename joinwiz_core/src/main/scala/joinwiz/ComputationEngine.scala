package joinwiz

import joinwiz.api.{Collect, Distinct, Filter, FlatMap, GroupByKey, Join, Map, UnionByName, WithWindow}

trait ComputationEngine[F[_]] extends Serializable {

  def join[T]: Join[F, T]

  def map[T]: Map[F, T]

  def flatMap[T]: FlatMap[F, T]

  def filter[T]: Filter[F, T]

  def distinct[T]: Distinct[F, T]

  def groupByKey[T]: GroupByKey[F, T]

  def unionByName[T]: UnionByName[F, T]

  def collect[T]: Collect[F, T]

  def withWindow[T]: WithWindow[F, T]
}
