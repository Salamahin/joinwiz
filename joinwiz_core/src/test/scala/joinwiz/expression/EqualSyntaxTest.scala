package joinwiz.expression

import joinwiz.TColumn
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class EqualSyntaxTest extends AnyFunSuite with Matchers {
  import joinwiz.syntax._

  private val evaluate = (TColumn.left[Left, Right], TColumn.right[Left, Right])

  case class Left(pk: String = null, opt: Option[String] = None)
  case class Right(pk: String = null, opt: Option[String] = None)

  test("left T and right T can equal") {
    val evaluated = evaluate match {
      case (left, right) => left(_.pk) =:= right(_.pk)
    }

    evaluated(Left(pk = "matching"), Right(pk = "matching")) should be(true)
    evaluated(Left(pk = "matching"), Right(pk = "non-matching")) should be(false)

    evaluated().expr.toString() should be("('LEFT.pk = 'RIGHT.pk)")
  }

  test("left T and right opt T can equal") {
    val evaluated = evaluate match {
      case (left, right) => left(_.pk) =:= right(_.opt)
    }

    evaluated(Left(pk = "matching"), Right(opt = Some("matching"))) should be(true)
    evaluated(Left(pk = "matching"), Right(opt = Some("non-matching"))) should be(false)
    evaluated(Left(pk = "matching"), Right(opt = None)) should be(false)

    evaluated().expr.toString() should be("('LEFT.pk = 'RIGHT.opt)")
  }

  test("left T and left T can equal") {
    val evaluated = evaluate match {
      case (left, _) => left(_.pk) =:= left(_.pk)
    }

    evaluated(Left(pk = "matching"), Right()) should be(true)

    evaluated().expr.toString() should be("('LEFT.pk = 'LEFT.pk)")
  }

  test("left T and left opt T can equal") {
    val evaluated = evaluate match {
      case (left, _) => left(_.pk) =:= left(_.opt)
    }

    evaluated(Left(pk = "matching", opt = Some("matching")), Right()) should be(true)
    evaluated(Left(pk = "matching", opt = Some("non-matching")), Right()) should be(false)
    evaluated(Left(pk = "matching", opt = None), Right()) should be(false)

    evaluated().expr.toString() should be("('LEFT.pk = 'LEFT.opt)")
  }

  test("left T and const T can equal") {
    val evaluated = evaluate match {
      case (left, _) => left(_.pk) =:= "matching"
    }

    evaluated(Left(pk = "matching"), Right()) should be(true)
    evaluated(Left(pk = "non-matching"), Right()) should be(false)

    evaluated().expr.toString() should be("('LEFT.pk = matching)")
  }

  test("right T and left T can equal") {
    val evaluated = evaluate match {
      case (left, right) => right(_.pk) =:= left(_.pk)
    }

    evaluated(Left(pk = "matching"), Right(pk = "matching")) should be(true)
    evaluated(Left(pk = "matching"), Right(pk = "non-matching")) should be(false)

    evaluated().expr.toString() should be("('RIGHT.pk = 'LEFT.pk)")
  }

  test("right T and left opt T can equal") {
    val evaluated = evaluate match {
      case (left, right) => right(_.pk) =:= left(_.opt)
    }

    evaluated(Left(opt = Some("matching")), Right(pk     = "matching")) should be(true)
    evaluated(Left(opt = Some("non-matching")), Right(pk = "matching")) should be(false)
    evaluated(Left(opt = None), Right(pk                 = "matching")) should be(false)

    evaluated().expr.toString() should be("('RIGHT.pk = 'LEFT.opt)")
  }

  test("right T and right T can equal") {
    val evaluated = evaluate match {
      case (_, right) => right(_.pk) =:= right(_.pk)
    }

    evaluated(Left(), Right(pk = "matching")) should be(true)

    evaluated().expr.toString() should be("('RIGHT.pk = 'RIGHT.pk)")
  }

  test("right T and right opt T can equal") {
    val evaluated = evaluate match {
      case (_, right) => right(_.pk) =:= right(_.opt)
    }

    evaluated(Left(), Right(pk = "matching", opt = Some("matching"))) should be(true)
    evaluated(Left(), Right(pk = "matching", opt = Some("non-matching"))) should be(false)
    evaluated(Left(), Right(pk = "matching", opt = None)) should be(false)

    evaluated().expr.toString() should be("('RIGHT.pk = 'RIGHT.opt)")
  }

  test("right T and const T can equal") {
    val evaluated = evaluate match {
      case (_, right) => right(_.pk) =:= "matching"
    }

    evaluated(Left(), Right(pk = "matching")) should be(true)
    evaluated(Left(), Right(pk = "non-matching")) should be(false)

    evaluated().expr.toString() should be("('RIGHT.pk = matching)")
  }

  test("left opt T and right T can equal") {
    val evaluated = evaluate match {
      case (left, right) => left(_.opt) =:= right(_.pk)
    }

    evaluated(Left(opt = Some("matching")), Right(pk     = "matching")) should be(true)
    evaluated(Left(opt = Some("matching")), Right(pk     = "non-matching")) should be(false)
    evaluated(Left(opt = Some("non-matching")), Right(pk = "matching")) should be(false)
    evaluated(Left(opt = None), Right(pk                 = "matching")) should be(false)

    evaluated().expr.toString() should be("('LEFT.opt = 'RIGHT.pk)")
  }

  test("left opt T and right opt T can equal") {
    val evaluated = evaluate match {
      case (left, right) => left(_.opt) =:= right(_.opt)
    }

    evaluated(Left(opt = Some("matching")), Right(opt = Some("matching"))) should be(true)
    evaluated(Left(opt = Some("matching")), Right(opt = Some("non-matching"))) should be(false)
    evaluated(Left(opt = Some("matching")), Right(opt = None)) should be(false)
    evaluated(Left(opt = None), Right(opt             = None)) should be(false)

    evaluated().expr.toString() should be("('LEFT.opt = 'RIGHT.opt)")
  }

  test("left opt T and left T can equal") {
    val evaluated = evaluate match {
      case (left, _) => left(_.opt) =:= left(_.pk)
    }

    evaluated(Left(opt = Some("matching"), pk     = "matching"), Right()) should be(true)
    evaluated(Left(opt = Some("matching"), pk     = "non-matching"), Right()) should be(false)
    evaluated(Left(opt = Some("non-matching"), pk = "matching"), Right()) should be(false)
    evaluated(Left(opt = None, pk                 = "matching"), Right()) should be(false)

    evaluated().expr.toString() should be("('LEFT.opt = 'LEFT.pk)")
  }

  test("left opt T and left opt T can equal") {
    val evaluated = evaluate match {
      case (left, _) => left(_.opt) =:= left(_.opt)
    }

    evaluated(Left(opt = Some("matching")), Right()) should be(true)
    evaluated(Left(opt = None), Right()) should be(false)

    evaluated().expr.toString() should be("('LEFT.opt = 'LEFT.opt)")
  }

  test("left opt T and const T can equal") {
    val evaluated = evaluate match {
      case (left, _) => left(_.opt) =:= "matching"
    }

    evaluated(Left(opt = Some("matching")), Right()) should be(true)
    evaluated(Left(opt = Some("non-matching")), Right()) should be(false)
    evaluated(Left(opt = None), Right()) should be(false)

    evaluated().expr.toString() should be("('LEFT.opt = matching)")
  }

  test("right opt T and left T can equal") {
    val evaluated = evaluate match {
      case (left, right) => right(_.pk) =:= left(_.opt)
    }

    evaluated(Left(opt = Some("matching")), Right(pk     = "matching")) should be(true)
    evaluated(Left(opt = Some("matching")), Right(pk     = "non-matching")) should be(false)
    evaluated(Left(opt = Some("non-matching")), Right(pk = "matching")) should be(false)
    evaluated(Left(opt = None), Right(pk                 = "matching")) should be(false)

    evaluated().expr.toString() should be("('RIGHT.pk = 'LEFT.opt)")
  }

  test("right opt T and left opt T can equal") {
    val evaluated = evaluate match {
      case (left, right) => right(_.opt) =:= left(_.opt)
    }

    evaluated(Left(opt = Some("matching")), Right(opt = Some("matching"))) should be(true)
    evaluated(Left(opt = Some("matching")), Right(opt = Some("non-matching"))) should be(false)
    evaluated(Left(opt = Some("matching")), Right(opt = None)) should be(false)
    evaluated(Left(opt = None), Right(opt             = None)) should be(false)

    evaluated().expr.toString() should be("('RIGHT.opt = 'LEFT.opt)")
  }

  test("right opt T and right T can equal") {
    val evaluated = evaluate match {
      case (_, right) => right(_.opt) =:= right(_.pk)
    }

    evaluated(Left(), Right(opt = Some("matching"), pk     = "matching")) should be(true)
    evaluated(Left(), Right(opt = Some("matching"), pk     = "non-matching")) should be(false)
    evaluated(Left(), Right(opt = Some("non-matching"), pk = "matching")) should be(false)
    evaluated(Left(), Right(opt = None, pk                 = "matching")) should be(false)

    evaluated().expr.toString() should be("('RIGHT.opt = 'RIGHT.pk)")
  }

  test("right opt T and right opt T can equal") {
    val evaluated = evaluate match {
      case (_, right) => right(_.opt) =:= right(_.opt)
    }

    evaluated(Left(), Right(opt = Some("matching"))) should be(true)
    evaluated(Left(), Right(opt = None)) should be(false)

    evaluated().expr.toString() should be("('RIGHT.opt = 'RIGHT.opt)")
  }

  test("right opt T and const T can equal") {
    val evaluated = evaluate match {
      case (_, right) => right(_.opt) =:= "matching"
    }

    evaluated(Left(), Right(opt = Some("matching"))) should be(true)
    evaluated(Left(), Right(opt = Some("non-matching"))) should be(false)
    evaluated(Left(), Right(opt = None)) should be(false)

    evaluated().expr.toString() should be("('RIGHT.opt = matching)")
  }

}
