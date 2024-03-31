package joinwiz

import org.apache.spark.sql.functions.col
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class TWindowTest extends AnyFunSuite with Matchers {
  case class PartitionByTestee(a: String, b: Int, c: Boolean, d: Double)
  case class OrderByTestee(first: Int, second: Int, third: Int)

  test("can chain partition by in the window") {
    val window = new ApplyTWindow[PartitionByTestee]
      .partitionBy(_.a)
      .partitionBy(_.b)
      .partitionBy(_.c)
      .partitionBy(_.d)

    val testee = PartitionByTestee("string", -1, true, Math.PI)

    window(testee) should be(((("string", -1), true), Math.PI))
    window.partitionByCols should contain inOrderOnly (
      col("a"),
      col("b"),
      col("c"),
      col("d")
    )
  }

  test("can compose orderings") {
    val firstAsc   = Ordering.by[OrderByTestee, Int](_.first)
    val secondDesc = Ordering.by[OrderByTestee, Int](_.second).reverse

    val testee = TWindow.composeOrdering(firstAsc, secondDesc)

    val t1 = OrderByTestee(1, 1, 1)
    val t2 = OrderByTestee(1, 2, 1)

    List(t1, t2).sorted(testee) should contain inOrderOnly (t2, t1)
  }

  test("can chain asc and desc orderings") {
    val window = new ApplyTWindow[OrderByTestee]
      .partitionBy(_.first)
      .orderByAsc(_.first)
      .orderByAsc(_.second)
      .orderByDesc(_.third)

    val t1 = OrderByTestee(1, 1, 1)
    val t2 = OrderByTestee(2, 2, 2)
    val t3 = OrderByTestee(1, 2, 2)
    val t4 = OrderByTestee(1, 2, 3)
    val t5 = OrderByTestee(1, 1, 3)

    val ordered = window
      .ordering
      .map(ord => Set(t1, t2, t3, t4, t5).toList.sorted(ord))
      .get

    ordered should contain inOrderOnly (t5, t1, t4, t3, t2)
  }
}
