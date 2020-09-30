package joinwiz.expression

import joinwiz.{ApplyLTCol, ApplyRTCol}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class IsEmptySyntaxTest extends AnyFunSuite with Matchers {
  case class Left(opt: Option[String] = None)
  case class Right(opt: Option[String] = None)

  private val evaluate = (ApplyLTCol[Left, Right], ApplyRTCol[Left, Right])

  import joinwiz.syntax._

  test("left opt T can be empty") {
    val evaluated = evaluate match {
      case (l, _) => l(_.opt).isEmpty
    }

    evaluated(Left(None), Right()) should be(true)
    evaluated(Left(Some("non-empty")), Right()) should be(false)

    evaluated().expr.toString() should be("isnull('LEFT.opt)")
  }

  test("left opt T can be defined") {
    val evaluated = evaluate match {
      case (l, _) => l(_.opt).isDefined
    }

    evaluated(Left(None), Right()) should be(false)
    evaluated(Left(Some("non-empty")), Right()) should be(true)

    evaluated().expr.toString() should be("isnotnull('LEFT.opt)")
  }

  test("right opt T can be empty") {
    val evaluated = evaluate match {
      case (_, r) => r(_.opt).isEmpty
    }

    evaluated(Left(), Right(None)) should be(true)
    evaluated(Left(), Right(Some("non-empty"))) should be(false)

    evaluated().expr.toString() should be("isnull('RIGHT.opt)")
  }

  test("right opt T can be defined") {
    val evaluated = evaluate match {
      case (_, r) => r(_.opt).isDefined
    }

    evaluated(Left(), Right(None)) should be(false)
    evaluated(Left(), Right(Some("non-empty"))) should be(true)

    evaluated().expr.toString() should be("isnotnull('RIGHT.opt)")
  }
}
