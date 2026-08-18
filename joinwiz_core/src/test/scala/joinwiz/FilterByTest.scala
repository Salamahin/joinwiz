package joinwiz

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

object FilterByTest {
  case class Row(id: Int, name: String, opt: Option[Int])
}

class FilterByTest extends AnyFunSuite with Matchers with SparkSuite {
  import FilterByTest._
  import joinwiz.syntax._
  import joinwiz.spark._
  import ss.implicits._

  private val rows = Seq(Row(1, "a", Some(10)), Row(2, "b", None), Row(3, "a", Some(30)))

  test("filterBy lowers to a Catalyst Filter (pushdown-capable), unlike the opaque closure filter") {
    val byColumn  = rows.toDS().filterBy(r => r(_.id) > 1)
    val byClosure = rows.toDS().filter(_.id > 1)

    // Use the analyzed (pre-optimization) plan: over a LocalRelation the optimizer would otherwise
    // constant-fold the column predicate away entirely (ConvertToLocalRelation) — itself proof it is a
    // real Catalyst predicate — whereas the opaque TypedFilter always survives.
    val columnPlan  = byColumn.queryExecution.analyzed.toString()
    val closurePlan = byClosure.queryExecution.analyzed.toString()

    // Column path is a real Catalyst predicate on `id`; the closure path is a black-box TypedFilter.
    columnPlan should include("Filter")
    columnPlan should not include ("TypedFilter")
    closurePlan should include("TypedFilter")
  }

  test("filterBy matches the equivalent closure filter, including OR and Option fields") {
    def expected(r: Row) = r.id > 1 || r.name == "a"
    rows.toDS().filterBy(r => (r(_.id) > 1) || (r(_.name) =:= "a")).collect().toSeq should
      contain theSameElementsAs rows.filter(expected)

    // Option field vs constant: only defined values passing the predicate survive (null-safe, matches Spark).
    rows.toDS().filterBy(r => r(_.opt) > 10).collect().toSeq should
      contain theSameElementsAs rows.filter(_.opt.exists(_ > 10))
  }

  test("compare in filterBy is rejected at compile time for non-ordered element types") {
    val r = FColumn[Row]
    assertCompiles("r(_.id) > 1")             // Int is ordered
    assertCompiles("r(_.name) =:= \"a\"")     // equality works for any element type
    assertDoesNotCompile("r(_.name) > \"a\"") // String is not in the SparkOrdered set
    assertDoesNotCompile("r(_.name) > r(_.id)")
  }
}
