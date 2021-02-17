package joinwiz

import org.apache.spark.sql.functions.col
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

case class Testee(a: String, b: Int, c: Boolean, d: Double)

class TWindowTest extends AnyFunSuite with Matchers {

  test("can chain partition by in the window") {
    val window = new ApplyTWindow[Testee]
      .partitionBy(_.a)
      .partitionBy(_.b)
      .partitionBy(_.c)
      .partitionBy(_.d)

    val testee = Testee("string", -1, true, Math.PI)

    window(testee) should be(((("string", -1), true), Math.PI))
    window.partitionByCols
    window.partitionByCols should contain inOrderOnly (
      col("a"),
      col("b"),
      col("c"),
      col("d")
    )
  }
}
