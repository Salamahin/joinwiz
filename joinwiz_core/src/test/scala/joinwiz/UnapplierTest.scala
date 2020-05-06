package joinwiz

import joinwiz.UnapplierTest.{A, B, C}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

private object UnapplierTest {

  case class A(a: String)

  case class B(b: String)

  case class C(c: String)

}

class UnapplierTest extends AnyFunSuite with Matchers {
  private type ABC = ((A, B), C)

  private val aval = "aval"
  private val bval = "bval"
  private val cval = "cval"
  private val abc  = ((A(aval), B(bval)), C(cval))

  private val testee = (ApplyToLeftColumn[ABC], ApplyToRightColumn[C])

  test("unapplying a from ABC is not affecting scope") {
    val tCol = testee match {
      case (left(left(a, _), _), _) => a(_.a)
    }

    tCol.toString should be("left(_1._1.a)")
    tCol(abc) should be(aval)
  }

  test("unapplying b from ABC is not affecting scope") {
    val tCol = testee match {
      case (left(left(_, b), _), _) => b(_.b)
    }

    tCol.toString should be("left(_1._2.b)")
    tCol(abc) should be(bval)
  }

}
