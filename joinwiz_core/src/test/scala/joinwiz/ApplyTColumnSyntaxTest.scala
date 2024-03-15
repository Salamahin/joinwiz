package joinwiz

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

case class Outer(inner1: Inner, maybeInner2: Option[Inner])
case class Inner(value: String, maybeValue: Option[String])

class ApplyTColumnSyntaxTest extends AnyFunSuite with Matchers with ApplyTColumnSyntax {
  private val testee = Outer(
    inner1 = Inner(
      value      = "value1",
      maybeValue = Some("inner2")
    ),
    maybeInner2 = Some(
      Inner(
        value      = "value3",
        maybeValue = Some("value4")
      )
    )
  )

  test("LTColumn can extract a value and a path from a lambda") {
    val li = TColumn.left[Outer, Outer](_.inner1)

    li.get(testee) shouldBe testee.inner1
    li.path shouldBe ("LEFT" :: "inner1" :: Nil)
  }

  test("LTColumn can extract value sequentially in case of nested structures") {
    val li = TColumn.left[Outer, Outer](_.inner1)(_.value)

    li.get(testee) shouldBe testee.inner1.value
    li.path shouldBe ("LEFT" :: "inner1" :: "value" :: Nil)
  }

  test("LTColumn can extract a value from an optional nested struct without calling a .map on it") {
    val li = TColumn.left[Outer, Outer] >> (_.maybeInner2) >> ((inner: Inner) => inner.value)

    li.get(testee) shouldBe testee.maybeInner2.map(_.value)
    li.path shouldBe ("LEFT" :: "maybeInner2" :: "value" :: Nil)
  }

  test("LTColumn can extract an optional value from an optional nested struct without calling a .flatMap on it") {
    val li = TColumn.left[Outer, Outer] >> (_.maybeInner2) >> ((inner: Inner) => inner.maybeValue)

    li.get(testee) shouldBe testee.maybeInner2.flatMap(_.maybeValue)
    li.path shouldBe ("LEFT" :: "maybeInner2" :: "maybeValue" :: Nil)
  }

  test("RTColumn can extract a value and a path from a lambda") {
    val ri = TColumn.right[Outer, Outer](_.inner1)

    ri.get(testee) shouldBe testee.inner1
    ri.path shouldBe ("RIGHT" :: "inner1" :: Nil)
  }

  test("RTColumn can extract value sequentially in case of nested structures") {
    val ri = TColumn.right[Outer, Outer](_.inner1)(_.value)

    ri.get(testee) shouldBe testee.inner1.value
    ri.path shouldBe ("RIGHT" :: "inner1" :: "value" :: Nil)
  }

  test("RTColumn can extract a value from an optional nested struct without calling a .map on it") {
    val ri = TColumn.right[Outer, Outer] >> (_.maybeInner2) >> ((inner: Inner) => inner.value)

    ri.get(testee) shouldBe testee.maybeInner2.map(_.value)
    ri.path shouldBe ("RIGHT" :: "maybeInner2" :: "value" :: Nil)
  }

  test("RTColumn can extract an optional value from an optional nested struct without calling a .flatMap on it") {
    val ri = TColumn.right[Outer, Outer] >> (_.maybeInner2) >> ((inner: Inner) => inner.maybeValue)

    ri.get(testee) shouldBe testee.maybeInner2.flatMap(_.maybeValue)
    ri.path shouldBe ("RIGHT" :: "maybeInner2" :: "maybeValue" :: Nil)
  }
}
