package joinwiz.expression

import joinwiz.{ApplyLTCol2, ApplyRTCol2}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class EqualSyntaxTest extends AnyFunSuite with Matchers {
  import joinwiz.syntax2._

  case class A(pk: String = null, optional: Option[String] = None)
  case class B(pk: String = null, optional: Option[String] = None)

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
      case (left, right) => left(_.pk) =:= right(_.optional)
    }

    evaluated(A(pk = "matching"), B(optional = Some("matching"))) should be(true)
    evaluated(A(pk = "matching"), B(optional = Some("non-matching"))) should be(false)
    evaluated(A(pk = "matching"), B(optional = None)) should be(false)

    evaluated.expr.expr.toString() should be("('LEFT.pk = 'RIGHT.optional)")
  }

  test("left opt T and right opt T can equal") {
    val evaluated = evaluate match {
      case (left, right) => left(_.optional) =:= right(_.optional)
    }

    evaluated(A(optional = Some("matching")), B(optional = Some("matching"))) should be(true)
    evaluated(A(optional = Some("matching")), B(optional = Some("non-matching"))) should be(false)
    evaluated(A(optional = Some("matching")), B(optional = None)) should be(false)
    evaluated(A(optional = None), B(optional = None)) should be(false)

    evaluated.expr.expr.toString() should be("('LEFT.optional = 'RIGHT.optional)")
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
