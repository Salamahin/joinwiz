package joinwiz.expression

import joinwiz.{ApplyLTCol, ApplyRTCol}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class MapSyntaxTest extends AnyFunSuite with Matchers {
  private val evaluate = (ApplyLTCol[Left, Right], ApplyRTCol[Left, Right])

  case class Left(pk: Int = 0, opt: Option[Int] = None)
  case class Right(pk: Int = 0, opt: Option[Int] = None)

  import joinwiz.syntax._

  test("can map a left T") {
    val evaluated = evaluate match {
      case (left, right) => left(_.pk).map(_ + 1, _ + 1) =:= right(_.pk)
    }

    evaluated(Left(pk = 1), Right(pk = 2)) should be(true)
    evaluated(Left(pk = 2), Right(pk = 2)) should be(false)

    evaluated().expr.toString() should be("(('LEFT.pk + 1) = 'RIGHT.pk)")
  }

  test("can map a right T") {
    val evaluated = evaluate match {
      case (left, right) => left(_.pk) =:= right(_.pk).map(_ + 1, _ + 1)
    }

    evaluated(Left(pk = 2), Right(pk = 1)) should be(true)
    evaluated(Left(pk = 2), Right(pk = 2)) should be(false)

    evaluated().expr.toString() should be("('LEFT.pk = ('RIGHT.pk + 1))")
  }

  test("can map a left opt T") {
    val evaluated = evaluate match {
      case (left, right) => left(_.opt).map(_ + 1, _ + 1) =:= right(_.pk)
    }

    evaluated(Left(opt = Some(1)), Right(pk = 2)) should be(true)
    evaluated(Left(opt = Some(2)), Right(pk = 2)) should be(false)

    evaluated().expr.toString() should be("(('LEFT.opt + 1) = 'RIGHT.pk)")
  }

  test("can map a right opt T") {
    val evaluated = evaluate match {
      case (left, right) => left(_.pk) =:= right(_.opt).map(_ + 1, _ + 1)
    }

    evaluated(Left(pk = 2), Right(opt = Some(1))) should be(true)
    evaluated(Left(pk = 2), Right(opt = Some(2))) should be(false)

    evaluated().expr.toString() should be("('LEFT.pk = ('RIGHT.opt + 1))")
  }
}
