package law

import joinwiz.{LTColumnExtractor, RTColumnExtractor, Un}
import law.UnapplierTest.{A, B, C}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

object UnapplierTest {

  case class A(a: String)

  case class B(b: String)

  case class C(c: String)

}

class UnapplierTest extends AnyFunSuite with Matchers {
  private type ABC = ((A, B), C)

  private val aval = "aval"
  private val bval = "bval"
  private val cval = "cval"
  private val abc = ((A(aval), B(bval)), C(cval))

  private val testee = (new LTColumnExtractor[ABC, ABC](extractor = identity), new RTColumnExtractor[C])

  test("unapplying left A for ((a, b), c) should yield _1._1.a") {
    val tCol = testee match {
      case (Un(Un(a, _), _), _) => a(_.a)
    }

    tCol.toString should be("left(_1._1.a)")
    tCol(abc) should be(aval)
  }

  test("unapplying left B for ((a, b), c) should yield _1._2.b") {
    val tCol = testee match {
      case (Un(Un(_, b), _), _) => b(_.b)
    }

    tCol.toString should be("left(_1._2.b)")
    tCol(abc) should be(bval)
  }
}
