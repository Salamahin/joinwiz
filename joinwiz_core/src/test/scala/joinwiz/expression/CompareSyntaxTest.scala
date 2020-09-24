package joinwiz.expression

import joinwiz.{ApplyLTCol2, ApplyRTCol2}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CompareSyntaxTest extends AnyFunSuite with Matchers {
  import joinwiz.syntax2._

  case class Left(pk: Int = Int.MinValue, opt: Option[Int] = None)
  case class Right(pk: Int = Int.MinValue, opt: Option[Int] = None)

  private val evaluate = (ApplyLTCol2[Left], ApplyRTCol2[Right])

  test("left T can be < than right T") {
    val evaluated = evaluate match {
      case (left, right) => left(_.pk) < right(_.pk)
    }

    evaluated(Left(pk = 1), Right(pk = 1)) should be(false)
    evaluated(Left(pk = 0), Right(pk = 1)) should be(true)

    evaluated.expr.expr.toString() should be("('LEFT.pk < 'RIGHT.pk)")
  }

  test("left T can be <= than right T") {
    val evaluated = evaluate match {
      case (left, right) => left(_.pk) <= right(_.pk)
    }

    evaluated(Left(pk = 1), Right(pk = 1)) should be(true)
    evaluated(Left(pk = 0), Right(pk = 1)) should be(true)
    evaluated(Left(pk = 0), Right(pk = -1)) should be(false)

    evaluated.expr.expr.toString() should be("('LEFT.pk <= 'RIGHT.pk)")
  }

  test("left T can be > than right T") {
    val evaluated = evaluate match {
      case (left, right) => left(_.pk) > right(_.pk)
    }

    evaluated(Left(pk = 1), Right(pk = 1)) should be(false)
    evaluated(Left(pk = 1), Right(pk = 0)) should be(true)

    evaluated.expr.expr.toString() should be("('LEFT.pk > 'RIGHT.pk)")
  }

  test("left T can be >= than right T") {
    val evaluated = evaluate match {
      case (left, right) => left(_.pk) >= right(_.pk)
    }

    evaluated(Left(pk = 1), Right(pk = 1)) should be(true)
    evaluated(Left(pk = 1), Right(pk = 0)) should be(true)
    evaluated(Left(pk = -1), Right(pk = 0)) should be(false)

    evaluated.expr.expr.toString() should be("('LEFT.pk >= 'RIGHT.pk)")
  }

  test("left T can be < than right opt T") {
    val evaluated = evaluate match {
      case (left, right) => left(_.pk) < right(_.opt)
    }

    evaluated(Left(pk = 1), Right(opt = Some(1))) should be(false)
    evaluated(Left(pk = 0), Right(opt = Some(1))) should be(true)
    evaluated(Left(pk = 0), Right(opt = None)) should be(false)

    evaluated.expr.expr.toString() should be("('LEFT.pk < 'RIGHT.opt)")
  }

  test("left T can be <= than right opt T") {
    val evaluated = evaluate match {
      case (left, right) => left(_.pk) <= right(_.opt)
    }

    evaluated(Left(pk = 1), Right(opt = Some(1))) should be(true)
    evaluated(Left(pk = 0), Right(opt = Some(1))) should be(true)
    evaluated(Left(pk = 0), Right(opt = Some(-1))) should be(false)
    evaluated(Left(pk = 0), Right(opt = None)) should be(false)

    evaluated.expr.expr.toString() should be("('LEFT.pk <= 'RIGHT.opt)")
  }

  test("left T can be > than right opt T") {
    val evaluated = evaluate match {
      case (left, right) => left(_.pk) > right(_.opt)
    }

    evaluated(Left(pk = 1), Right(opt = Some(1))) should be(false)
    evaluated(Left(pk = 1), Right(opt = Some(0))) should be(true)
    evaluated(Left(pk = 1), Right(opt = None)) should be(false)

    evaluated.expr.expr.toString() should be("('LEFT.pk > 'RIGHT.opt)")
  }

  test("left T can be >= than right opt T") {
    val evaluated = evaluate match {
      case (left, right) => left(_.pk) >= right(_.opt)
    }

    evaluated(Left(pk = 1), Right(opt = Some(1))) should be(true)
    evaluated(Left(pk = 1), Right(opt = Some(0))) should be(true)
    evaluated(Left(pk = -1), Right(opt = Some(0))) should be(false)
    evaluated(Left(pk = -1), Right(opt = None)) should be(false)

    evaluated.expr.expr.toString() should be("('LEFT.pk >= 'RIGHT.opt)")
  }

  test("left T can be < than const") {
    val evaluated = evaluate match {
      case (left, _) => left(_.pk).<[Right](1)
    }

    evaluated(Left(pk = 1), Right()) should be(false)
    evaluated(Left(pk = 0), Right()) should be(true)

    evaluated.expr.expr.toString() should be("('LEFT.pk < 1)")
  }

  test("left T can be <= than const") {
    val evaluated = evaluate match {
      case (left, _) => left(_.pk).<=[Right](1)
    }

    evaluated(Left(pk = 1), Right()) should be(true)
    evaluated(Left(pk = 0), Right()) should be(true)
    evaluated(Left(pk = 2), Right()) should be(false)

    evaluated.expr.expr.toString() should be("('LEFT.pk <= 1)")
  }

  test("left T can be > than const") {
    val evaluated = evaluate match {
      case (left, _) => left(_.pk).>[Right](1)
    }

    evaluated(Left(pk = 1), Right()) should be(false)
    evaluated(Left(pk = 2), Right()) should be(true)

    evaluated.expr.expr.toString() should be("('LEFT.pk > 1)")
  }

  test("left T can be >= than const") {
    val evaluated = evaluate match {
      case (left, _) => left(_.pk).>=[Right](1)
    }

    evaluated(Left(pk = 1), Right()) should be(true)
    evaluated(Left(pk = 2), Right()) should be(true)
    evaluated(Left(pk = 0), Right()) should be(false)

    evaluated.expr.expr.toString() should be("('LEFT.pk >= 1)")
  }

  test("left opt T can be < than right T") {
    val evaluated = evaluate match {
      case (left, right) => left(_.opt) < right(_.pk)
    }

    evaluated(Left(opt = Some(1)), Right(pk = 1)) should be(false)
    evaluated(Left(opt = Some(0)), Right(pk = 1)) should be(true)
    evaluated(Left(opt = None), Right(pk = 1)) should be(false)

    evaluated.expr.expr.toString() should be("('LEFT.opt < 'RIGHT.pk)")
  }

  test("left opt T can be <= than right T") {
    val evaluated = evaluate match {
      case (left, right) => left(_.opt) <= right(_.pk)
    }

    evaluated(Left(opt = Some(1)), Right(pk = 1)) should be(true)
    evaluated(Left(opt = Some(0)), Right(pk = 1)) should be(true)
    evaluated(Left(opt = Some(0)), Right(pk = -1)) should be(false)
    evaluated(Left(opt = None), Right(pk = -1)) should be(false)

    evaluated.expr.expr.toString() should be("('LEFT.opt <= 'RIGHT.pk)")
  }

  test("left opt T can be > than right T") {
    val evaluated = evaluate match {
      case (left, right) => left(_.opt) > right(_.pk)
    }

    evaluated(Left(opt = Some(1)), Right(pk = 1)) should be(false)
    evaluated(Left(opt = Some(1)), Right(pk = 0)) should be(true)
    evaluated(Left(opt = None), Right(pk = 0)) should be(false)

    evaluated.expr.expr.toString() should be("('LEFT.opt > 'RIGHT.pk)")
  }

  test("left opt T can be >= than right T") {
    val evaluated = evaluate match {
      case (left, right) => left(_.opt) >= right(_.pk)
    }

    evaluated(Left(opt = Some(1)), Right(pk = 1)) should be(true)
    evaluated(Left(opt = Some(1)), Right(pk = 0)) should be(true)
    evaluated(Left(opt = Some(-1)), Right(pk = 0)) should be(false)
    evaluated(Left(opt = None), Right(pk = 0)) should be(false)

    evaluated.expr.expr.toString() should be("('LEFT.opt >= 'RIGHT.pk)")
  }

  test("left opt T can be < than right opt T") {
    val evaluated = evaluate match {
      case (left, right) => left(_.opt) < right(_.opt)
    }

    evaluated(Left(opt = Some(1)), Right(opt = Some(1))) should be(false)
    evaluated(Left(opt = Some(0)), Right(opt = Some(1))) should be(true)
    evaluated(Left(opt = Some(0)), Right(opt = None)) should be(false)
    evaluated(Left(opt = None), Right(opt = None)) should be(false)

    evaluated.expr.expr.toString() should be("('LEFT.opt < 'RIGHT.opt)")
  }

  test("left opt T can be <= than right opt T") {
    val evaluated = evaluate match {
      case (left, right) => left(_.opt) <= right(_.opt)
    }

    evaluated(Left(opt = Some(1)), Right(opt = Some(1))) should be(true)
    evaluated(Left(opt = Some(0)), Right(opt = Some(1))) should be(true)
    evaluated(Left(opt = Some(0)), Right(opt = Some(-1))) should be(false)
    evaluated(Left(opt = Some(0)), Right(opt = None)) should be(false)
    evaluated(Left(opt = None), Right(opt = None)) should be(false)

    evaluated.expr.expr.toString() should be("('LEFT.opt <= 'RIGHT.opt)")
  }

  test("left opt T can be > than right opt T") {
    val evaluated = evaluate match {
      case (left, right) => left(_.opt) > right(_.opt)
    }

    evaluated(Left(opt = Some(1)), Right(opt = Some(1))) should be(false)
    evaluated(Left(opt = Some(1)), Right(opt = Some(0))) should be(true)
    evaluated(Left(opt = Some(1)), Right(opt = None)) should be(false)
    evaluated(Left(opt = None), Right(opt = None)) should be(false)

    evaluated.expr.expr.toString() should be("('LEFT.opt > 'RIGHT.opt)")
  }

  test("left opt T can be >= than right opt T") {
    val evaluated = evaluate match {
      case (left, right) => left(_.opt) >= right(_.opt)
    }

    evaluated(Left(opt = Some(1)), Right(opt = Some(1))) should be(true)
    evaluated(Left(opt = Some(1)), Right(opt = Some(0))) should be(true)
    evaluated(Left(opt = Some(-1)), Right(opt = Some(0))) should be(false)
    evaluated(Left(opt = Some(-1)), Right(opt = None)) should be(false)
    evaluated(Left(opt = None), Right(opt = None)) should be(false)

    evaluated.expr.expr.toString() should be("('LEFT.opt >= 'RIGHT.opt)")
  }

  test("left opt T can be < than const") {
    val evaluated = evaluate match {
      case (left, _) => left(_.opt).<[Right](1)
    }

    evaluated(Left(opt = Some(1)), Right()) should be(false)
    evaluated(Left(opt = Some(0)), Right()) should be(true)
    evaluated(Left(opt = None), Right()) should be(false)

    evaluated.expr.expr.toString() should be("('LEFT.opt < 1)")
  }

  test("left opt T can be <= than const") {
    val evaluated = evaluate match {
      case (left, _) => left(_.opt).<=[Right](1)
    }

    evaluated(Left(opt = Some(1)), Right()) should be(true)
    evaluated(Left(opt = Some(0)), Right()) should be(true)
    evaluated(Left(opt = Some(2)), Right()) should be(false)
    evaluated(Left(opt = None), Right()) should be(false)

    evaluated.expr.expr.toString() should be("('LEFT.opt <= 1)")
  }

  test("left opt T can be > than const") {
    val evaluated = evaluate match {
      case (left, _) => left(_.opt).>[Right](1)
    }

    evaluated(Left(opt = Some(1)), Right()) should be(false)
    evaluated(Left(opt = Some(2)), Right()) should be(true)
    evaluated(Left(opt = None), Right()) should be(false)

    evaluated.expr.expr.toString() should be("('LEFT.opt > 1)")
  }

  test("left opt T can be >= than const") {
    val evaluated = evaluate match {
      case (left, _) => left(_.opt).>=[Right](1)
    }

    evaluated(Left(opt = Some(1)), Right()) should be(true)
    evaluated(Left(opt = Some(2)), Right()) should be(true)
    evaluated(Left(opt = Some(0)), Right()) should be(false)
    evaluated(Left(opt = None), Right()) should be(false)

    evaluated.expr.expr.toString() should be("('LEFT.opt >= 1)")
  }
}
