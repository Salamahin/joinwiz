package joinwiz.spark

import joinwiz.DatasetOperations
import org.apache.spark.sql.Dataset

object implicits {
  implicit val api: DatasetOperations[Dataset] = SparkOperations
}
