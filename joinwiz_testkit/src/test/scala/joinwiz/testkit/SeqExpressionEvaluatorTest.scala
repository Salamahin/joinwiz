package joinwiz.testkit

import joinwiz.testkit.SeqExpressionEvaluatorTest._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

object SeqExpressionEvaluatorTest {
  case class A(value: Int)
  case class B(value: Int)
}

class SeqExpressionEvaluatorTest extends AnyFunSuite with Matchers {
  val as = Seq(A(0), A(1))
  val bs = Seq(B(1), B(2))

  import joinwiz.syntax._
  import joinwiz.testkit.implicits._

  test("can check left == right") {
    as.innerJoin(bs)((l, r) => l(_.value) =:= r(_.value)) should contain only ((A(1), B(1)))
  }

  test("can check left < right") {
    as.innerJoin(bs)((l, r) => l(_.value) < r(_.value)) should contain only (
      (A(0), B(1)),
      (A(0), B(2)),
      (A(1), B(2))
    )
  }

  test("can check left > right") {
    as.innerJoin(bs)((l, r) => l(_.value) > r(_.value)) should be('empty)
  }

  test("can check left >= right") {
    as.innerJoin(bs)((l, r) => l(_.value) >= r(_.value)) should contain only ((A(1), B(1)))
  }

  test("can check left <= right") {
    as.innerJoin(bs)((l, r) => l(_.value) <= r(_.value)) should contain only (
      (A(0), B(1)),
      (A(0), B(2)),
      (A(1), B(1)),
      (A(1), B(2))
    )
  }

  test("can check left == const") {
    as.innerJoin(bs)((l, _) => l(_.value) =:= 1) should contain only (
      (A(1), B(1)),
      (A(1), B(2))
    )
  }

  test("can check left < const") {
    as.innerJoin(bs)((l, _) => l(_.value) < 1) should contain only (
      (A(0), B(1)),
      (A(0), B(2))
    )
  }

  test("can check left > const") {
    as.innerJoin(bs)((l, _) => l(_.value) > 0) should contain only (
      (A(1), B(1)),
      (A(1), B(2))
    )
  }

  test("can check left >= const") {
    as.innerJoin(bs)((l, _) => l(_.value) >= 1) should contain only (
      (A(1), B(1)),
      (A(1), B(2))
    )
  }

  test("can check left <= const") {
    as.innerJoin(bs)((l, _) => l(_.value) <= 0) should contain only (
      (A(0), B(1)),
      (A(0), B(2))
    )
  }

  test("can check right == const") {
    as.innerJoin(bs)((_, r) => r(_.value) =:= 1) should contain only (
      (A(0), B(1)),
      (A(1), B(1))
    )
  }

  test("can check right < const") {
    as.innerJoin(bs)((_, r) => r(_.value) < 2) should contain only (
      (A(0), B(1)),
      (A(1), B(1))
    )
  }

  test("can check right > const") {
    as.innerJoin(bs)((_, r) => r(_.value) > 1) should contain only (
      (A(0), B(2)),
      (A(1), B(2))
    )
  }

  test("can check right >= const") {
    as.innerJoin(bs)((_, r) => r(_.value) >= 2) should contain only (
      (A(0), B(2)),
      (A(1), B(2))
    )
  }

  test("can check right <= const") {
    as.innerJoin(bs)((_, r) => r(_.value) <= 1) should contain only (
      (A(0), B(1)),
      (A(1), B(1))
    )
  }

}
