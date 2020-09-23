package joinwiz.expression

import joinwiz.{ApplyLTCol2, ApplyRTCol2, Expr}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class EqualSyntaxTest extends AnyFunSuite with Matchers {
  import joinwiz.syntax2._

  case class A(pk: String = null, opt: Option[String] = None)
  case class B(pk: String = null, opt: Option[String] = None)

  private val evaluate = (ApplyLTCol2[A], ApplyRTCol2[B])

  test("left T and right T can equal") {
    val evaluated = evaluate match {
      case (left, right) => left(_.pk) =:= right(_.pk)
    }

    evaluated(A(pk = "matching"), B(pk = "matching")) should be(true)
    evaluated(A(pk = "matching"), B(pk = "non-matching")) should be(false)

    evaluated.expr.expr.toString() should be("('LEFT.pk = 'RIGHT.pk)")
  }

  test("left T and right opt T can equal") {
    val evaluated = evaluate match {
      case (left, right) => left(_.pk) =:= right(_.opt)
    }

    evaluated(A(pk = "matching"), B(opt = Some("matching"))) should be(true)
    evaluated(A(pk = "matching"), B(opt = Some("non-matching"))) should be(false)
    evaluated(A(pk = "matching"), B(opt = None)) should be(false)

    evaluated.expr.expr.toString() should be("('LEFT.pk = 'RIGHT.opt)")
  }

  test("left T and left T can equal") {
    val evaluated = evaluate match {
      case (left, _) => left(_.pk).=:=[B](left(_.pk))
    }

    evaluated(A(pk = "matching"), B()) should be(true)

    evaluated.expr.expr.toString() should be("('LEFT.pk = 'LEFT.pk)")
  }

  test("left T and left opt T can equal") {
    val evaluated = evaluate match {
      case (left, _) => left(_.pk).=:=[B](left(_.opt))
    }

    evaluated(A(pk = "matching", opt = Some("matching")), B()) should be(true)
    evaluated(A(pk = "matching", opt = Some("non-matching")), B()) should be(false)
    evaluated(A(pk = "matching", opt = None), B()) should be(false)

    evaluated.expr.expr.toString() should be("('LEFT.pk = 'LEFT.opt)")
  }

  test("left T and const T can equal") {
    val evaluated = evaluate match {
      case (left, _) => left(_.pk).=:=[B]("matching")
    }

    evaluated(A(pk = "matching"), B()) should be(true)
    evaluated(A(pk = "non-matching"), B()) should be(false)

    evaluated.expr.expr.toString() should be("('LEFT.pk = matching)")
  }

  test("left opt T and right T can equal") {
    val evaluated = evaluate match {
      case (left, right) => left(_.opt) =:= right(_.pk)
    }

    evaluated(A(opt = Some("matching")), B(pk = "matching")) should be(true)
    evaluated(A(opt = Some("matching")), B(pk = "non-matching")) should be(false)
    evaluated(A(opt = Some("non-matching")), B(pk = "matching")) should be(false)
    evaluated(A(opt = None), B(pk = "matching")) should be(false)

    evaluated.expr.expr.toString() should be("('LEFT.opt = 'RIGHT.pk)")
  }

  test("left opt T and right opt T can equal") {
    val evaluated = evaluate match {
      case (left, right) => left(_.opt) =:= right(_.opt)
    }

    evaluated(A(opt = Some("matching")), B(opt = Some("matching"))) should be(true)
    evaluated(A(opt = Some("matching")), B(opt = Some("non-matching"))) should be(false)
    evaluated(A(opt = Some("matching")), B(opt = None)) should be(false)
    evaluated(A(opt = None), B(opt = None)) should be(false)

    evaluated.expr.expr.toString() should be("('LEFT.opt = 'RIGHT.opt)")
  }

  test("left opt T and left T can equal") {
    val evaluated = evaluate match {
      case (left, _) => left(_.opt).=:=[B](left(_.pk))
    }

    evaluated(A(opt = Some("matching"), pk = "matching"), B()) should be(true)
    evaluated(A(opt = Some("matching"), pk = "non-matching"), B()) should be(false)
    evaluated(A(opt = Some("non-matching"), pk = "matching"), B()) should be(false)
    evaluated(A(opt = None, pk = "matching"), B()) should be(false)

    evaluated.expr.expr.toString() should be("('LEFT.opt = 'LEFT.pk)")
  }

  test("left opt T and left opt T can equal") {
    val evaluated = evaluate match {
      case (left, _) => left(_.opt).=:=[B](left(_.opt))
    }

    evaluated(A(opt = Some("matching")), B()) should be(true)
    evaluated(A(opt = None), B()) should be(false)

    evaluated.expr.expr.toString() should be("('LEFT.opt = 'LEFT.opt)")
  }

  test("left opt T and const T can equal") {
    val evaluated = evaluate match {
      case (left, _) => left(_.opt).=:=[B]("matching")
    }

    evaluated(A(opt = Some("matching")), B()) should be(true)
    evaluated(A(opt = Some("non-matching")), B()) should be(false)
    evaluated(A(opt = None), B()) should be(false)

    evaluated.expr.expr.toString() should be("('LEFT.opt = matching)")
  }

//
//  evaluate match {
//    case (left, _) => left(_.pk) =:= "1"
//  }
//
//  evaluate match {
//    case (left, _) => left(_.optional) =:= "1"
//  }
//
//  evaluate match {
//    case (left, right) => right(_.pk) =:= left(_.pk)
//  }
//
//  evaluate match {
//    case (left, right) => right(_.pk) =:= left(_.optional)
//  }
//
//  evaluate match {
//    case (left, right) => right(_.optional) =:= left(_.optional)
//  }
//
//  evaluate match {
//    case (_, right) => right(_.pk) =:= "1"
//  }
//
//  evaluate match {
//    case (_, right) => right(_.optional) =:= "1"
//  }

}
