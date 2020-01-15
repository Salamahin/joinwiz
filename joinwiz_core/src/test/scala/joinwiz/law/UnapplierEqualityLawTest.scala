package joinwiz.law

import joinwiz.{LTColumnExtractor, RTColumnExtractor, left}
import joinwiz.law.UnapplierEqualityLawTest.{A, B, C}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

private object UnapplierEqualityLawTest {

  case class A(aString: String, aOptString: Option[String], aDecimal: BigDecimal)

  case class B(bString: String, bOptString: Option[String], bDecimal: BigDecimal)

  case class C(cString: String, cOptString: Option[String], cDecimal: BigDecimal)

}

class UnapplierEqualityLawTest extends AnyFunSuite with Matchers {
  private type ABC = ((A, B), C)

  private val testee = (LTColumnExtractor[ABC], RTColumnExtractor[C])

  import joinwiz.syntax._

  test("left T and right T can build an equality") {
    (testee match {
      case (left(left(a, _), _), c) => a(_.aString) =:= c(_.cString)
    }).toString should be("left(_1._1.aString) =:= right(cString)")
  }

  test("left T and right Option[T] can build an equality") {
    (testee match {
      case (left(left(a, _), _), c) => a(_.aString) =:= c(_.cOptString)
    }).toString should be("left(_1._1.aString) =:= right(cOptString)")
  }

  test("right Option[T] and left T can build an equality") {
    (testee match {
      case (left(left(_, b), _), c) => b(_.bString) =:= c(_.cString)
    }).toString should be("left(_1._2.bString) =:= right(cString)")
  }

  test("left T and const T can build an equality") {
    (testee match {
      case (left(left(_, b), _), _) => b(_.bString) =:= "hello"
    }).toString should be("left(_1._2.bString) =:= const(hello)")
  }

  test("equality is commutative") {
    (testee match {
      case (left(left(_, b), _), _) => "hello" =:= b(_.bString)
    }).toString should be("left(_1._2.bString) =:= const(hello)")
  }

  test("equality is contravariant") {
    (testee match {
      case (left(left(_, b), _), _) => b(_.bOptString) =:= None
    }).toString should be("left(_1._2.bOptString) =:= const(None)")
  }
}
