package joinwiz.testkit.sparkless

import joinwiz.testkit.DatasetOperations

object implicits {
  implicit val api: DatasetOperations[Seq] = SparklessOperations
}
