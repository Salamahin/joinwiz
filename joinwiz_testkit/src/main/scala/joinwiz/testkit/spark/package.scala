package joinwiz.testkit

import org.apache.spark.sql.Dataset

package object spark {
  implicit val api: DatasetOperations[Dataset] = SparkOperations
}
