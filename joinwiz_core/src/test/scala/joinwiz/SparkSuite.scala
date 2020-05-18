package joinwiz

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SparkSuite extends BeforeAndAfterAll {
  this: Suite =>

  @transient private var _ss: SparkSession = _

  lazy val ss: SparkSession = _ss

  override def beforeAll() {
    super.beforeAll()

    _ss = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.driver.host", "127.0.0.1")
      .getOrCreate()
  }
}
