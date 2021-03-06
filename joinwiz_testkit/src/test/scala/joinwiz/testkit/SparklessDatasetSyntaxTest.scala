package joinwiz.testkit

import joinwiz.ComputationEngineTest

import scala.reflect.runtime.universe.TypeTag

class SparklessDatasetSyntaxTest extends ComputationEngineTest[Seq] {
  override def entities[T <: Product: TypeTag](a: T*): Seq[T] = a.toSeq
}
