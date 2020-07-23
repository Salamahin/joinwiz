package joinwiz.testkit

import joinwiz.testkit.SeqExpressionEvaluatorTest._
import joinwiz.wiz
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

object SeqExpressionEvaluatorTest {
  case class A(value: Int)
  case class B(value: Int)
  case class C(optValue: Option[Int])
}

class SeqExpressionEvaluatorTest extends AnyFunSuite with Matchers {
  private val a0      = A(0)
  private val a1      = A(1)
  private val b1      = B(1)
  private val b2      = B(2)
  private val c1      = C(Some(1))
  private val c3      = C(Some(3))
  private val cminus1 = C(Some(-1))
  private val cnone   = C(None)

  val as = Seq(a0, a1)
  val bs = Seq(b1, b2)
  val cs = Seq(c1, c3, cminus1, cnone)

  import joinwiz.syntax._

  test("can check left == right") {
    as.innerJoin(bs)((l, r) => l(_.value) =:= r(_.value)) should contain only ((a1, b1))
  }

  test("can check left < right") {
    as.innerJoin(bs)((l, r) => l(_.value) < r(_.value)) should contain only (
      (a0, b1),
      (a0, b2),
      (a1, b2)
    )
  }

  test("can check left > right") {
    as.innerJoin(bs)((l, r) => l(_.value) > r(_.value)) should be('empty)
  }

  test("can check left >= right") {
    as.innerJoin(bs)((l, r) => l(_.value) >= r(_.value)) should contain only ((a1, b1))
  }

  test("can check left <= right") {
    as.innerJoin(bs)((l, r) => l(_.value) <= r(_.value)) should contain only (
      (a0, b1),
      (a0, b2),
      (a1, b1),
      (a1, b2)
    )
  }

  test("can check left == const") {
    as.innerJoin(bs)((l, _) => l(_.value) =:= 1) should contain only (
      (a1, b1),
      (a1, b2)
    )
  }

  test("can check left < const") {
    as.innerJoin(bs)((l, _) => l(_.value) < 1) should contain only (
      (a0, b1),
      (a0, b2)
    )
  }

  test("can check left > const") {
    as.innerJoin(bs)((l, _) => l(_.value) > 0) should contain only (
      (a1, b1),
      (a1, b2)
    )
  }

  test("can check left >= const") {
    as.innerJoin(bs)((l, _) => l(_.value) >= 1) should contain only (
      (a1, b1),
      (a1, b2)
    )
  }

  test("can check left <= const") {
    as.innerJoin(bs)((l, _) => l(_.value) <= 0) should contain only (
      (a0, b1),
      (a0, b2)
    )
  }

  test("can check right == const") {
    as.innerJoin(bs)((_, r) => r(_.value) =:= 1) should contain only (
      (a0, b1),
      (a1, b1)
    )
  }

  test("can check right < const") {
    as.innerJoin(bs)((_, r) => r(_.value) < 2) should contain only (
      (a0, b1),
      (a1, b1)
    )
  }

  test("can check right > const") {
    as.innerJoin(bs)((_, r) => r(_.value) > 1) should contain only (
      (a0, b2),
      (a1, b2)
    )
  }

  test("can check right >= const") {
    as.innerJoin(bs)((_, r) => r(_.value) >= 2) should contain only (
      (a0, b2),
      (a1, b2)
    )
  }

  test("can check right <= const") {
    as.innerJoin(bs)((_, r) => r(_.value) <= 1) should contain only (
      (a0, b1),
      (a1, b1)
    )
  }

  test("can join on lifted to some value when equals") {
    as.innerJoin(cs) {
      case (a, c) => a(_.value).some =:= c(_.optValue)
    } should contain only ((a1, c1))
  }

  test("can join on lifted to some value when greater or eq") {
    as.innerJoin(cs) {
      case (a, c) => a(_.value).some >= c(_.optValue)
    } should contain only (
      (a0, cminus1),
      (a1, c1),
      (a1, cminus1)
    )
  }

  test("can join on lifted to some value when greater") {
    as.innerJoin(cs) {
      case (a, c) => a(_.value).some > c(_.optValue)
    } should contain only (
      (a0, cminus1),
      (a1, cminus1)
    )
  }

  test("can join on lifted to some value when less or eq") {
    as.innerJoin(cs) {
      case (a, c) => a(_.value).some <= c(_.optValue)
    } should contain only (
      (a0, c1),
      (a0, c3),
      (a1, c1),
      (a1, c3)
    )
  }

  test("can join on lifted to some value when less") {
    as.innerJoin(cs) {
      case (a, c) => a(_.value).some < c(_.optValue)
    } should contain only (
      (a0, c1),
      (a0, c3),
      (a1, c3)
    )
  }

  test("can join on const when eq") {
    as.innerJoin(bs) {
      case (a, _) => a(_.value) =:= 1
    } should contain only (
      (a1, b1),
      (a1, b2)
    )
  }

  test("can join on const when less") {
    as.innerJoin(bs) {
      case (a, _) => a(_.value) < 1
    } should contain only (
      (a0, b1),
      (a0, b2)
    )
  }

  test("can join on const when great") {
    as.innerJoin(bs) {
      case (a, _) => a(_.value) > 0
    } should contain only (
      (a1, b1),
      (a1, b2)
    )
  }

  test("can join on const when less or eq") {
    as.innerJoin(bs) {
      case (a, _) => a(_.value) <= 1
    } should contain only (
      (a0, b1),
      (a0, b2),
      (a1, b1),
      (a1, b2)
    )
  }

  test("can join on const when great or eq") {
    as.innerJoin(bs) {
      case (a, _) => a(_.value) >= 0
    } should contain only (
      (a0, b1),
      (a0, b2),
      (a1, b1),
      (a1, b2)
    )
  }

  test("can join unapplying ApplyLeft `wiz`") {
    (as zip bs)
      .innerJoin(cs) {
        case (a wiz _, c) => a(_.value).some =:= c(_.optValue)
      } should contain only (((a1, b2), c1))
  }

  test("can join unapplying ApplyRight`wiz`") {
    as
      .innerJoin(bs zip cs) {
        case (a, _ wiz c) => a(_.value).some =:= c(_.optValue)
      } should contain only ((a1, (b1, c1)))
  }
}
