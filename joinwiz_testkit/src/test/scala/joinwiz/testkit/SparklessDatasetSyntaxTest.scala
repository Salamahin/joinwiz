package joinwiz.testkit

import joinwiz.ComputationEngineTest

class SparklessDatasetSyntaxTest extends ComputationEngineTest[Seq] {
  override def entities(a: ComputationEngineTest.Entity*): Seq[ComputationEngineTest.Entity] = a.toSeq
}
