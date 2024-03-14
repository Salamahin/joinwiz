package joinwiz

import joinwiz.expression.UnapplySyntax2
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class UnapplySyntax2Test extends AnyFunSuite with Matchers with UnapplySyntax2 with ApplyTColumnSyntax {
  case class A(valueA: String)
  case class B(valueB: String)

  private val testeeA = A("value1")
  private val testeeB = B("value2")

  test("LTColumn can unapply tupled values") {
    val (a, b) = TColumn.left[(A, B)] match {
      case a wiz2 b => (a(_.valueA), b(_.valueB))
    }

    a.get((testeeA, testeeB)) shouldBe "value1"
    a.path shouldBe ("LEFT" :: "_1" :: "valueA" :: Nil)

    b.get((testeeA, testeeB)) shouldBe "value2"
    b.path shouldBe ("LEFT" :: "_2" :: "valueB" :: Nil)
  }

  test("RTColumn can unapply tupled values") {
    val (a, b) = TColumn.right[(A, B)] match {
      case a wiz2 b => (a(_.valueA), b(_.valueB))
    }

    a.get((testeeA, testeeB)) shouldBe "value1"
    a.path shouldBe ("RIGHT" :: "_1" :: "valueA" :: Nil)

    b.get((testeeA, testeeB)) shouldBe "value2"
    b.path shouldBe ("RIGHT" :: "_2" :: "valueB" :: Nil)
  }
}
