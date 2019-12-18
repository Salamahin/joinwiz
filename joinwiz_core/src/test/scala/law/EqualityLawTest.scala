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
    }) should be(Equality(LeftField("aString"), RightField("bString")))
  }

  test("left T and right Option[T] can build an equality") {
    (testee match {
      case (left, right) => left(_.aString) =:= right(_.bOptString)
    }) should be(Equality(LeftField("aString"), RightField("bOptString")))
  }

  test("right Option[T] and left T can build an equality") {
    (testee match {
      case (left, right) => left(_.aOptString) =:= right(_.bString)
    }) should be(Equality(LeftField("aOptString"), RightField("bString")))
  }

  test("left T and const T can build an equality") {
    (testee match {
      case (left, _) => left(_.aString) =:= "hello"
    }) should be(Equality(LeftField("aString"), Const[String]("hello")))
  }

  test("equality is commutative") {
    (testee match {
      case (left, _) => "hello" =:= left(_.aString)
    }) should be(Equality(LeftField("aString"), Const[String]("hello")))
  }
}
