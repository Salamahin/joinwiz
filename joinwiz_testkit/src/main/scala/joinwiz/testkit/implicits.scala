package joinwiz.testkit

import joinwiz.DatasetOperations

object implicits {
  implicit val api: DatasetOperations[Seq] = SparklessOperations
}
