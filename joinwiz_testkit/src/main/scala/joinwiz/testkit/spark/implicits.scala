package joinwiz.testkit.spark

import joinwiz.testkit.DatasetOperations
import org.apache.spark.sql.Dataset

object implicits {
  implicit val api: DatasetOperations[Dataset] = SparkOperations
}
