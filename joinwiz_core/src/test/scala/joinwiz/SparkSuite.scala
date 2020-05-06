package joinwiz

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SparkSuite extends BeforeAndAfterAll {
  self: Suite =>

  @transient private var _ss: SparkSession = _

  def ss: SparkSession = _ss

  override def beforeAll() {
    super.beforeAll()

    _ss = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.driver.host", "127.0.0.1")
      .getOrCreate()
  }

  override def afterAll() {
    try {
      _ss.close()
    } finally {
      super.afterAll()
    }
  }
}
