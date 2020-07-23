package joinwiz

import joinwiz.UnapplicationTest._
import joinwiz.spark.SparkExpressionEvaluator
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

private object UnapplicationTest {
  case class A(aString: String, aOptString: Option[String])
  case class B(bString: String, bOptString: Option[String])
  case class C(cString: String, cOptString: Option[String])
  case class D(dString: String, dOptString: Option[String])
}

class UnapplicationTest extends AnyFunSuite with Matchers {
  private type ABC = ((A, B), C)
  private type BCD = (B, (C, D))

  private val leftTestee  = (ApplyLeft[ABC], ApplyRight[D])
  private val rightTestee = (ApplyLeft[A], ApplyRight[BCD])

  import joinwiz.syntax._

  test("left unapplication of the joined entity does not affect the scope") {
    SparkExpressionEvaluator
      .evaluate(leftTestee match {
        case (a wiz _ wiz _, d) => a(_.aString) =:= d(_.dString)
      })
      .expr
      .toString() should be("('LEFT._1._1.aString = 'RIGHT.dString)")
  }

  test("right unapplication of the joined entity does not affect the scope") {
    SparkExpressionEvaluator
      .evaluate(rightTestee match {
        case (a, wiz(_, wiz(_, d))) => a(_.aString) =:= d(_.dString)
      })
      .expr
      .toString() should be("('LEFT.aString = 'RIGHT._2._2.dString)")
  }
}
