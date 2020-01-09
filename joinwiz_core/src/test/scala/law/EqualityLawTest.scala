package law

import joinwiz._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class EqualityLawTest extends AnyFunSuite with Matchers {

  case class A(aString: String, aOptString: Option[String], aDecimal: BigDecimal)

  case class B(bString: String, bOptString: Option[String], bDecimal: BigDecimal)

  import JoinWiz._

  private val testee = (new LTColumnExtractor[A], new RTColumnExtractor[B])

  test("left T and right T can build an equality") {
    (testee match {
      case (left, right) => left(_.aString) =:= right(_.bString)
    }).toString should be("left(aString) =:= right(bString)")
  }

  test("left T and right Option[T] can build an equality") {
    (testee match {
      case (left, right) => left(_.aString) =:= right(_.bOptString)
    }).toString should be("left(aString) =:= right(bOptString)")
  }

  test("right Option[T] and left T can build an equality") {
    (testee match {
      case (left, right) => left(_.aOptString) =:= right(_.bString)
    }).toString should be("left(aOptString) =:= right(bString)")
  }

  test("left T and const T can build an equality") {
    (testee match {
      case (left, _) => left(_.aString) =:= "hello"
    }).toString should be("left(aString) =:= const(hello)")
  }

  test("equality is commutative") {
    (testee match {
      case (left, _) => "hello" =:= left(_.aString)
    }).toString should be("left(aString) =:= const(hello)")
  }
}
