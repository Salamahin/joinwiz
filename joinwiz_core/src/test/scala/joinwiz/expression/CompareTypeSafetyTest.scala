package joinwiz.expression

import joinwiz.TColumn
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.sql.Date

class CompareTypeSafetyTest extends AnyFunSuite with Matchers {
  import joinwiz.syntax._

  case class Sub(x: Int)
  case class Left(pk: Int, name: String, flag: Boolean, d: Date, sub: Sub, optI: Option[Int], optName: Option[String])
  case class Right(pk: Int, name: String, flag: Boolean, d: Date, sub: Sub, optI: Option[Int], optName: Option[String])

  private val l = TColumn.left[Left, Right]
  private val r = TColumn.right[Left, Right]

  test("ordered element types support compare") {
    assertCompiles("l(_.pk) < r(_.pk)")
    assertCompiles("l(_.d) <= r(_.d)")
    assertCompiles("l(_.pk) > 5")
    assertCompiles("l(_.optI) < r(_.optI)")
  }

  test("equality is available for every element type") {
    assertCompiles("l(_.name) =:= r(_.name)")
    assertCompiles("l(_.flag) =:= r(_.flag)")
    assertCompiles("l(_.sub) =:= r(_.sub)")
    assertCompiles("l(_.optName) =:= r(_.optName)")
  }

  test("compare is rejected for non-ordered element types") {
    assertDoesNotCompile("l(_.name) < r(_.name)")
    assertDoesNotCompile("l(_.flag) < r(_.flag)")
    assertDoesNotCompile("l(_.sub) < r(_.sub)")
    assertDoesNotCompile("l(_.optName) < r(_.optName)")
  }

  test("compare is rejected when the two sides have different element types") {
    assertDoesNotCompile("l(_.pk) < r(_.name)")
    assertDoesNotCompile("""l(_.pk) < "oops"""")
  }
}
