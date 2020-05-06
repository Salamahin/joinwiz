package joinwiz.law

import joinwiz.law.CanEqualTest.{A, B}
import joinwiz.{ApplyToLeftColumn, ApplyToRightColumn}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

private object CanEqualTest {

  case class A(aString: String, aOptString: Option[String], aDecimal: BigDecimal)

  case class B(bString: String, bOptString: Option[String], bDecimal: BigDecimal)

}

class CanEqualTest extends AnyFunSuite with Matchers {

  private val testee = (ApplyToLeftColumn[A], ApplyToRightColumn[B])

  import joinwiz.syntax._

  test("can equal left T and right T") {
    (testee match {
      case (left, right) => left(_.aString) =:= right(_.bString)
    }).toString should be("left(aString) =:= right(bString)")
  }

  test("can equal right T and left T") {
    (testee match {
      case (left, right) => right(_.bString) =:= left(_.aString)
    }).toString should be("left(aString) =:= right(bString)")
  }

  test("can equal left T and right Option[T] after left being lifted to some") {
    (testee match {
      case (left, right) => left(_.aString).some =:= right(_.bOptString)
    }).toString should be("left(aString) =:= right(bOptString)")
  }

  test("can equal left T and right Option[T] after right being lifted to some") {
    (testee match {
      case (left, right) => left(_.aOptString) =:= right(_.bString).some
    }).toString should be("left(aOptString) =:= right(bString)")
  }

  test("can equal left T and const T") {
    (testee match {
      case (left, _) => left(_.aString) =:= "hello"
    }).toString should be("left(aString) =:= const(hello)")
  }

  test("can equal right T and const T") {
    (testee match {
      case (_, right) => right(_.bString) =:= "hello"
    }).toString should be("right(bString) =:= const(hello)")
  }
}
