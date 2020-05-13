package joinwiz

import joinwiz.UnapplicationTest._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

private object UnapplicationTest {
  case class A(aString: String, aOptString: Option[String])
  case class B(bString: String, bOptString: Option[String])
  case class C(cString: String, cOptString: Option[String])
  case class D(dString: String, dOptString: Option[String])
}

class UnapplicationTest extends AnyFunSuite with Matchers {
  private type ABC = ((A, B), C)
  private type BCD = (B, (C, D))

  private val leftTestee  = (ApplyToLeftColumn[ABC], ApplyToRightColumn[D])
  private val rightTestee = (ApplyToLeftColumn[A], ApplyToRightColumn[BCD])

  import joinwiz.syntax._

  test("left unapplication of the joined entity does not affect the scope") {
    (leftTestee match {
      case (a wiz _ wiz _, d) => a(_.aString) =:= d(_.dString)
    }) should be(Equality(LeftTypedColumn(Seq("_1", "_1", "aString")), RightTypedColumn(Seq("dString"))))
  }

  test("right unapplication of the joined entity does not affect the scope") {
    (rightTestee match {
      case (a, wiz(_, wiz(_, d))) => a(_.aString) =:= d(_.dString)
    }) should be(Equality(LeftTypedColumn(Seq("aString")), RightTypedColumn(Seq("_2", "_2", "dString"))))
  }

  test("lifting left to some does not affect the scope") {
    (leftTestee match {
      case (wiz(wiz(a, _), _), d) => a(_.aString).some =:= d(_.dOptString)
    }) should be(Equality(LeftTypedColumn(Seq("_1", "_1", "aString")), RightTypedColumn(Seq("dOptString"))))
  }

  test("lifting right to some does not affect the scope") {
    (rightTestee match {
      case (a, wiz(_, wiz(_, d))) => a(_.aString).some =:= d(_.dOptString)
    }) should be(Equality(LeftTypedColumn(Seq("aString")), RightTypedColumn(Seq("_2", "_2", "dOptString"))))
  }

}
