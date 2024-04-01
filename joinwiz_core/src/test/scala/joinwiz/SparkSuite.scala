package joinwiz

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SparkSuite extends BeforeAndAfterAll {
  this: Suite =>

  lazy val ss: SparkSession                = _ss
  @transient private var _ss: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    _ss = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.sql.shuffle.partitions", 10)
      .config("spark.sql.crossJoin.enabled", "true")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }
}
