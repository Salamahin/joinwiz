package joinwiz

import joinwiz.api.{Collect, Distinct, Filter, FlatMap, GroupByKey, Join, Map, UnionByName, WithWindow}

trait ComputationEngine[F[_]] extends Serializable {
  def map: Map[F]
  def flatMap: FlatMap[F]
  def filter: Filter[F]
  def distinct: Distinct[F]
  def collect: Collect[F]
  def groupByKey: GroupByKey[F]
  def join: Join[F]
  def unionByName: UnionByName[F]
  def withWindow: WithWindow[F]
}
