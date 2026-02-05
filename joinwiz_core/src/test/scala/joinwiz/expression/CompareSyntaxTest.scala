package joinwizession

import joinwiz.TColumn
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CompareSyntaxTest extends AnyFunSuite with Matchers {
  import joinwiz.syntax._

  private val evaluate = (TColumn.left[Left, Right], TColumn.right[Left, Right])

  case class Left(pk: Int = Int.MinValue, opt: Option[Int] = None)

  case class Right(pk: Int = Int.MinValue, opt: Option[Int] = None)

  test("left T can be < than right T") {
    val evaluated = evaluate match {
      case (left, right) => left(_.pk) < right(_.pk)
    }

    evaluated(Left(pk = 1), Right(pk = 1)) should be(false)
    evaluated(Left(pk = 0), Right(pk = 1)) should be(true)

    evaluated().toString() should be("(LEFT.pk < RIGHT.pk)")
  }

  test("left T can be <= than right T") {
    val evaluated = evaluate match {
      case (left, right) => left(_.pk) <= right(_.pk)
    }

    evaluated(Left(pk = 1), Right(pk = 1)) should be(true)
    evaluated(Left(pk = 0), Right(pk = 1)) should be(true)
    evaluated(Left(pk = 0), Right(pk = -1)) should be(false)

    evaluated().toString() should be("(LEFT.pk <= RIGHT.pk)")
  }

  test("left T can be > than right T") {
    val evaluated = evaluate match {
      case (left, right) => left(_.pk) > right(_.pk)
    }

    evaluated(Left(pk = 1), Right(pk = 1)) should be(false)
    evaluated(Left(pk = 1), Right(pk = 0)) should be(true)

    evaluated().toString() should be("(LEFT.pk > RIGHT.pk)")
  }

  test("left T can be >= than right T") {
    val evaluated = evaluate match {
      case (left, right) => left(_.pk) >= right(_.pk)
    }

    evaluated(Left(pk = 1), Right(pk  = 1)) should be(true)
    evaluated(Left(pk = 1), Right(pk  = 0)) should be(true)
    evaluated(Left(pk = -1), Right(pk = 0)) should be(false)

    evaluated().toString() should be("(LEFT.pk >= RIGHT.pk)")
  }

  test("left T can be < than right opt T") {
    val evaluated = evaluate match {
      case (left, right) => left(_.pk) < right(_.opt)
    }

    evaluated(Left(pk = 1), Right(opt = Some(1))) should be(false)
    evaluated(Left(pk = 0), Right(opt = Some(1))) should be(true)
    evaluated(Left(pk = 0), Right(opt = None)) should be(false)

    evaluated().toString() should be("(LEFT.pk < RIGHT.opt)")
  }

  test("left T can be <= than right opt T") {
    val evaluated = evaluate match {
      case (left, right) => left(_.pk) <= right(_.opt)
    }

    evaluated(Left(pk = 1), Right(opt = Some(1))) should be(true)
    evaluated(Left(pk = 0), Right(opt = Some(1))) should be(true)
    evaluated(Left(pk = 0), Right(opt = Some(-1))) should be(false)
    evaluated(Left(pk = 0), Right(opt = None)) should be(false)

    evaluated().toString() should be("(LEFT.pk <= RIGHT.opt)")
  }

  test("left T can be > than right opt T") {
    val evaluated = evaluate match {
      case (left, right) => left(_.pk) > right(_.opt)
    }

    evaluated(Left(pk = 1), Right(opt = Some(1))) should be(false)
    evaluated(Left(pk = 1), Right(opt = Some(0))) should be(true)
    evaluated(Left(pk = 1), Right(opt = None)) should be(false)

    evaluated().toString() should be("(LEFT.pk > RIGHT.opt)")
  }

  test("left T can be >= than right opt T") {
    val evaluated = evaluate match {
      case (left, right) => left(_.pk) >= right(_.opt)
    }

    evaluated(Left(pk = 1), Right(opt  = Some(1))) should be(true)
    evaluated(Left(pk = 1), Right(opt  = Some(0))) should be(true)
    evaluated(Left(pk = -1), Right(opt = Some(0))) should be(false)
    evaluated(Left(pk = -1), Right(opt = None)) should be(false)

    evaluated().toString() should be("(LEFT.pk >= RIGHT.opt)")
  }

  test("left T can be < than const") {
    val evaluated = evaluate match {
      case (left, _) => left(_.pk) < 1
    }

    evaluated(Left(pk = 1), Right()) should be(false)
    evaluated(Left(pk = 0), Right()) should be(true)

    evaluated().toString() should be("(LEFT.pk < 1)")
  }

  test("left T can be <= than const") {
    val evaluated = evaluate match {
      case (left, _) => left(_.pk) <= 1
    }

    evaluated(Left(pk = 1), Right()) should be(true)
    evaluated(Left(pk = 0), Right()) should be(true)
    evaluated(Left(pk = 2), Right()) should be(false)

    evaluated().toString() should be("(LEFT.pk <= 1)")
  }

  test("left T can be > than const") {
    val evaluated = evaluate match {
      case (left, _) => left(_.pk) > 1
    }

    evaluated(Left(pk = 1), Right()) should be(false)
    evaluated(Left(pk = 2), Right()) should be(true)

    evaluated().toString() should be("(LEFT.pk > 1)")
  }

  test("left T can be >= than const") {
    val evaluated = evaluate match {
      case (left, _) => left(_.pk) >= 1
    }

    evaluated(Left(pk = 1), Right()) should be(true)
    evaluated(Left(pk = 2), Right()) should be(true)
    evaluated(Left(pk = 0), Right()) should be(false)

    evaluated().toString() should be("(LEFT.pk >= 1)")
  }

  test("left opt T can be < than right T") {
    val evaluated = evaluate match {
      case (left, right) => left(_.opt) < right(_.pk)
    }

    evaluated(Left(opt = Some(1)), Right(pk = 1)) should be(false)
    evaluated(Left(opt = Some(0)), Right(pk = 1)) should be(true)
    evaluated(Left(opt = None), Right(pk    = 1)) should be(false)

    evaluated().toString() should be("(LEFT.opt < RIGHT.pk)")
  }

  test("left opt T can be <= than right T") {
    val evaluated = evaluate match {
      case (left, right) => left(_.opt) <= right(_.pk)
    }

    evaluated(Left(opt = Some(1)), Right(pk = 1)) should be(true)
    evaluated(Left(opt = Some(0)), Right(pk = 1)) should be(true)
    evaluated(Left(opt = Some(0)), Right(pk = -1)) should be(false)
    evaluated(Left(opt = None), Right(pk    = -1)) should be(false)

    evaluated().toString() should be("(LEFT.opt <= RIGHT.pk)")
  }

  test("left opt T can be > than right T") {
    val evaluated = evaluate match {
      case (left, right) => left(_.opt) > right(_.pk)
    }

    evaluated(Left(opt = Some(1)), Right(pk = 1)) should be(false)
    evaluated(Left(opt = Some(1)), Right(pk = 0)) should be(true)
    evaluated(Left(opt = None), Right(pk    = 0)) should be(false)

    evaluated().toString() should be("(LEFT.opt > RIGHT.pk)")
  }

  test("left opt T can be >= than right T") {
    val evaluated = evaluate match {
      case (left, right) => left(_.opt) >= right(_.pk)
    }

    evaluated(Left(opt = Some(1)), Right(pk  = 1)) should be(true)
    evaluated(Left(opt = Some(1)), Right(pk  = 0)) should be(true)
    evaluated(Left(opt = Some(-1)), Right(pk = 0)) should be(false)
    evaluated(Left(opt = None), Right(pk     = 0)) should be(false)

    evaluated().toString() should be("(LEFT.opt >= RIGHT.pk)")
  }

  test("left opt T can be < than right opt T") {
    val evaluated = evaluate match {
      case (left, right) => left(_.opt) < right(_.opt)
    }

    evaluated(Left(opt = Some(1)), Right(opt = Some(1))) should be(false)
    evaluated(Left(opt = Some(0)), Right(opt = Some(1))) should be(true)
    evaluated(Left(opt = Some(0)), Right(opt = None)) should be(false)
    evaluated(Left(opt = None), Right(opt    = None)) should be(false)

    evaluated().toString() should be("(LEFT.opt < RIGHT.opt)")
  }

  test("left opt T can be <= than right opt T") {
    val evaluated = evaluate match {
      case (left, right) => left(_.opt) <= right(_.opt)
    }

    evaluated(Left(opt = Some(1)), Right(opt = Some(1))) should be(true)
    evaluated(Left(opt = Some(0)), Right(opt = Some(1))) should be(true)
    evaluated(Left(opt = Some(0)), Right(opt = Some(-1))) should be(false)
    evaluated(Left(opt = Some(0)), Right(opt = None)) should be(false)
    evaluated(Left(opt = None), Right(opt    = None)) should be(false)

    evaluated().toString() should be("(LEFT.opt <= RIGHT.opt)")
  }

  test("left opt T can be > than right opt T") {
    val evaluated = evaluate match {
      case (left, right) => left(_.opt) > right(_.opt)
    }

    evaluated(Left(opt = Some(1)), Right(opt = Some(1))) should be(false)
    evaluated(Left(opt = Some(1)), Right(opt = Some(0))) should be(true)
    evaluated(Left(opt = Some(1)), Right(opt = None)) should be(false)
    evaluated(Left(opt = None), Right(opt    = None)) should be(false)

    evaluated().toString() should be("(LEFT.opt > RIGHT.opt)")
  }

  test("left opt T can be >= than right opt T") {
    val evaluated = evaluate match {
      case (left, right) => left(_.opt) >= right(_.opt)
    }

    evaluated(Left(opt = Some(1)), Right(opt  = Some(1))) should be(true)
    evaluated(Left(opt = Some(1)), Right(opt  = Some(0))) should be(true)
    evaluated(Left(opt = Some(-1)), Right(opt = Some(0))) should be(false)
    evaluated(Left(opt = Some(-1)), Right(opt = None)) should be(false)
    evaluated(Left(opt = None), Right(opt     = None)) should be(false)

    evaluated().toString() should be("(LEFT.opt >= RIGHT.opt)")
  }

  test("left opt T can be < than const") {
    val evaluated = evaluate match {
      case (left, _) => left(_.opt) < 1
    }

    evaluated(Left(opt = Some(1)), Right()) should be(false)
    evaluated(Left(opt = Some(0)), Right()) should be(true)
    evaluated(Left(opt = None), Right()) should be(false)

    evaluated().toString() should be("(LEFT.opt < 1)")
  }

  test("left opt T can be <= than const") {
    val evaluated = evaluate match {
      case (left, _) => left(_.opt) <= 1
    }

    evaluated(Left(opt = Some(1)), Right()) should be(true)
    evaluated(Left(opt = Some(0)), Right()) should be(true)
    evaluated(Left(opt = Some(2)), Right()) should be(false)
    evaluated(Left(opt = None), Right()) should be(false)

    evaluated().toString() should be("(LEFT.opt <= 1)")
  }

  test("left opt T can be > than const") {
    val evaluated = evaluate match {
      case (left, _) => left(_.opt) > 1
    }

    evaluated(Left(opt = Some(1)), Right()) should be(false)
    evaluated(Left(opt = Some(2)), Right()) should be(true)
    evaluated(Left(opt = None), Right()) should be(false)

    evaluated().toString() should be("(LEFT.opt > 1)")
  }

  test("left opt T can be >= than const") {
    val evaluated = evaluate match {
      case (left, _) => left(_.opt) >= 1
    }

    evaluated(Left(opt = Some(1)), Right()) should be(true)
    evaluated(Left(opt = Some(2)), Right()) should be(true)
    evaluated(Left(opt = Some(0)), Right()) should be(false)
    evaluated(Left(opt = None), Right()) should be(false)

    evaluated().toString() should be("(LEFT.opt >= 1)")
  }

  // tests for right
  test("right T can be < than left T") {
    val evaluated = evaluate match {
      case (left, right) => right(_.pk) < left(_.pk)
    }

    evaluated(Left(pk = 1), Right(pk = 1)) should be(false)
    evaluated(Left(pk = 1), Right(pk = 0)) should be(true)

    evaluated().toString() should be("(RIGHT.pk < LEFT.pk)")
  }

  test("right T can be <= than left T") {
    val evaluated = evaluate match {
      case (left, right) => right(_.pk) <= left(_.pk)
    }

    evaluated(Left(pk = 1), Right(pk = 1)) should be(true)
    evaluated(Left(pk = 1), Right(pk = 0)) should be(true)
    evaluated(Left(pk = 0), Right(pk = 1)) should be(false)

    evaluated().toString() should be("(RIGHT.pk <= LEFT.pk)")
  }

  test("right T can be > than left T") {
    val evaluated = evaluate match {
      case (left, right) => right(_.pk) > left(_.pk)
    }

    evaluated(Left(pk = 1), Right(pk = 1)) should be(false)
    evaluated(Left(pk = 0), Right(pk = 1)) should be(true)

    evaluated().toString() should be("(RIGHT.pk > LEFT.pk)")
  }

  test("right T can be >= than left T") {
    val evaluated = evaluate match {
      case (left, right) => right(_.pk) >= left(_.pk)
    }

    evaluated(Left(pk = 1), Right(pk = 1)) should be(true)
    evaluated(Left(pk = 0), Right(pk = 1)) should be(true)
    evaluated(Left(pk = 0), Right(pk = -1)) should be(false)

    evaluated().toString() should be("(RIGHT.pk >= LEFT.pk)")
  }

  test("right T can be < than left opt T") {
    val evaluated = evaluate match {
      case (left, right) => right(_.pk) < left(_.opt)
    }

    evaluated(Left(opt = Some(1)), Right(pk = 1)) should be(false)
    evaluated(Left(opt = Some(1)), Right(pk = 0)) should be(true)
    evaluated(Left(opt = None), Right(pk    = 1)) should be(false)

    evaluated().toString() should be("(RIGHT.pk < LEFT.opt)")
  }

  test("right T can be <= than left opt T") {
    val evaluated = evaluate match {
      case (left, right) => right(_.pk) <= left(_.opt)
    }

    evaluated(Left(opt = Some(1)), Right(pk  = 1)) should be(true)
    evaluated(Left(opt = Some(1)), Right(pk  = 0)) should be(true)
    evaluated(Left(opt = Some(-1)), Right(pk = 0)) should be(false)
    evaluated(Left(opt = None), Right(pk     = 0)) should be(false)

    evaluated().toString() should be("(RIGHT.pk <= LEFT.opt)")
  }

  test("right T can be > than left opt T") {
    val evaluated = evaluate match {
      case (left, right) => right(_.pk) > left(_.opt)
    }

    evaluated(Left(opt = Some(1)), Right(pk = 1)) should be(false)
    evaluated(Left(opt = Some(0)), Right(pk = 1)) should be(true)
    evaluated(Left(opt = None), Right(pk    = 1)) should be(false)

    evaluated().toString() should be("(RIGHT.pk > LEFT.opt)")
  }

  test("right T can be >= than left opt T") {
    val evaluated = evaluate match {
      case (left, right) => right(_.pk) >= left(_.opt)
    }

    evaluated(Left(opt = Some(1)), Right(pk = 1)) should be(true)
    evaluated(Left(opt = Some(0)), Right(pk = 1)) should be(true)
    evaluated(Left(opt = Some(0)), Right(pk = -1)) should be(false)
    evaluated(Left(opt = None), Right(pk    = -1)) should be(false)

    evaluated().toString() should be("(RIGHT.pk >= LEFT.opt)")
  }

  test("right T can be < than const") {
    val evaluated = evaluate match {
      case (_, right) => right(_.pk) < 1
    }

    evaluated(Left(), Right(pk = 1)) should be(false)
    evaluated(Left(), Right(pk = 0)) should be(true)

    evaluated().toString() should be("(RIGHT.pk < 1)")
  }

  test("right T can be <= than const") {
    val evaluated = evaluate match {
      case (_, right) => right(_.pk) <= 1
    }

    evaluated(Left(), Right(pk = 1)) should be(true)
    evaluated(Left(), Right(pk = 0)) should be(true)
    evaluated(Left(), Right(pk = 2)) should be(false)

    evaluated().toString() should be("(RIGHT.pk <= 1)")
  }

  test("right T can be > than const") {
    val evaluated = evaluate match {
      case (_, right) => right(_.pk) > 1
    }

    evaluated(Left(), Right(pk = 1)) should be(false)
    evaluated(Left(), Right(pk = 2)) should be(true)

    evaluated().toString() should be("(RIGHT.pk > 1)")
  }

  test("right T can be >= than const") {
    val evaluated = evaluate match {
      case (_, right) => right(_.pk) >= 1
    }

    evaluated(Left(), Right(pk = 1)) should be(true)
    evaluated(Left(), Right(pk = 2)) should be(true)
    evaluated(Left(), Right(pk = 0)) should be(false)

    evaluated().toString() should be("(RIGHT.pk >= 1)")
  }

  test("right opt T can be < than right T") {
    val evaluated = evaluate match {
      case (left, right) => right(_.opt) < left(_.pk)
    }

    evaluated(Left(pk = 1), Right(opt = Some(1))) should be(false)
    evaluated(Left(pk = 1), Right(opt = Some(0))) should be(true)
    evaluated(Left(pk = 1), Right(opt = None)) should be(false)

    evaluated().toString() should be("(RIGHT.opt < LEFT.pk)")
  }

  test("right opt T can be <= than left T") {
    val evaluated = evaluate match {
      case (left, right) => right(_.opt) <= left(_.pk)
    }

    evaluated(Left(pk = 1), Right(opt  = Some(1))) should be(true)
    evaluated(Left(pk = 1), Right(opt  = Some(0))) should be(true)
    evaluated(Left(pk = -1), Right(opt = Some(0))) should be(false)
    evaluated(Left(pk = -1), Right(opt = None)) should be(false)

    evaluated().toString() should be("(RIGHT.opt <= LEFT.pk)")
  }

  test("right opt T can be > than left T") {
    val evaluated = evaluate match {
      case (left, right) => right(_.opt) > left(_.pk)
    }

    evaluated(Left(pk = 1), Right(opt = Some(1))) should be(false)
    evaluated(Left(pk = 0), Right(opt = Some(1))) should be(true)
    evaluated(Left(pk = 0), Right(opt = None)) should be(false)

    evaluated().toString() should be("(RIGHT.opt > LEFT.pk)")
  }

  test("right opt T can be >= than left T") {
    val evaluated = evaluate match {
      case (left, right) => right(_.opt) >= left(_.pk)
    }

    evaluated(Left(pk = 1), Right(opt = Some(1))) should be(true)
    evaluated(Left(pk = 0), Right(opt = Some(1))) should be(true)
    evaluated(Left(pk = 0), Right(opt = Some(-1))) should be(false)
    evaluated(Left(pk = 0), Right(opt = None)) should be(false)

    evaluated().toString() should be("(RIGHT.opt >= LEFT.pk)")
  }

  test("right opt T can be < than left opt T") {
    val evaluated = evaluate match {
      case (left, right) => right(_.opt) < left(_.opt)
    }

    evaluated(Left(opt = Some(1)), Right(opt = Some(1))) should be(false)
    evaluated(Left(opt = Some(1)), Right(opt = Some(0))) should be(true)
    evaluated(Left(opt = None), Right(opt    = Some(0))) should be(false)
    evaluated(Left(opt = None), Right(opt    = None)) should be(false)

    evaluated().toString() should be("(RIGHT.opt < LEFT.opt)")
  }

  test("right opt T can be <= than left opt T") {
    val evaluated = evaluate match {
      case (left, right) => right(_.opt) <= left(_.opt)
    }

    evaluated(Left(opt = Some(1)), Right(opt  = Some(1))) should be(true)
    evaluated(Left(opt = Some(1)), Right(opt  = Some(0))) should be(true)
    evaluated(Left(opt = Some(-1)), Right(opt = Some(0))) should be(false)
    evaluated(Left(opt = None), Right(opt     = Some(0))) should be(false)
    evaluated(Left(opt = None), Right(opt     = None)) should be(false)

    evaluated().toString() should be("(RIGHT.opt <= LEFT.opt)")
  }

  test("right opt T can be > than left opt T") {
    val evaluated = evaluate match {
      case (left, right) => right(_.opt) > left(_.opt)
    }

    evaluated(Left(opt = Some(1)), Right(opt = Some(1))) should be(false)
    evaluated(Left(opt = Some(0)), Right(opt = Some(1))) should be(true)
    evaluated(Left(opt = None), Right(opt    = Some(1))) should be(false)
    evaluated(Left(opt = None), Right(opt    = None)) should be(false)

    evaluated().toString() should be("(RIGHT.opt > LEFT.opt)")
  }

  test("right opt T can be >= than left opt T") {
    val evaluated = evaluate match {
      case (left, right) => right(_.opt) >= left(_.opt)
    }

    evaluated(Left(opt = Some(1)), Right(opt = Some(1))) should be(true)
    evaluated(Left(opt = Some(0)), Right(opt = Some(1))) should be(true)
    evaluated(Left(opt = Some(0)), Right(opt = Some(-1))) should be(false)
    evaluated(Left(opt = None), Right(opt    = Some(-1))) should be(false)
    evaluated(Left(opt = None), Right(opt    = None)) should be(false)

    evaluated().toString() should be("(RIGHT.opt >= LEFT.opt)")
  }

  test("right opt T can be < than const") {
    val evaluated = evaluate match {
      case (_, right) => right(_.opt) < 1
    }

    evaluated(Left(), Right(opt = Some(1))) should be(false)
    evaluated(Left(), Right(opt = Some(0))) should be(true)
    evaluated(Left(), Right(opt = None)) should be(false)

    evaluated().toString() should be("(RIGHT.opt < 1)")
  }

  test("right opt T can be <= than const") {
    val evaluated = evaluate match {
      case (_, right) => right(_.opt) <= 1
    }

    evaluated(Left(), Right(opt = Some(1))) should be(true)
    evaluated(Left(), Right(opt = Some(0))) should be(true)
    evaluated(Left(), Right(opt = Some(2))) should be(false)
    evaluated(Left(), Right(opt = None)) should be(false)

    evaluated().toString() should be("(RIGHT.opt <= 1)")
  }

  test("right opt T can be > than const") {
    val evaluated = evaluate match {
      case (_, right) => right(_.opt) > 1
    }

    evaluated(Left(), Right(opt = Some(1))) should be(false)
    evaluated(Left(), Right(opt = Some(2))) should be(true)
    evaluated(Left(), Right(opt = None)) should be(false)

    evaluated().toString() should be("(RIGHT.opt > 1)")
  }

  test("right opt T can be >= than const") {
    val evaluated = evaluate match {
      case (_, right) => right(_.opt) >= 1
    }

    evaluated(Left(), Right(opt = Some(1))) should be(true)
    evaluated(Left(), Right(opt = Some(2))) should be(true)
    evaluated(Left(), Right(opt = Some(0))) should be(false)
    evaluated(Left(), Right(opt = None)) should be(false)

    evaluated().toString() should be("(RIGHT.opt >= 1)")
  }
}
