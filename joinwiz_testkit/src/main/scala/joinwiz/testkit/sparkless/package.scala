package joinwiz.testkit

package object sparkless {
  implicit val api: DatasetOperations[Seq] = SparklessOperations
}
