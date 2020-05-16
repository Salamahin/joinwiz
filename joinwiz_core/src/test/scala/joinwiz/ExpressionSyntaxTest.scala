package joinwiz

import joinwiz.ExpressionSyntaxTest._
import joinwiz.spark.SparkExpressionEvaluator
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

private object ExpressionSyntaxTest {
  case class A(aString: String, aOptString: Option[String], aDecimal: BigDecimal)
  case class B(bString: String, bOptString: Option[String], bDecimal: BigDecimal)
}

class ExpressionSyntaxTest extends AnyFunSuite with Matchers {

  private val testee = (ApplyLeft[A], ApplyRight[B])

  import joinwiz.syntax._

  test("can equal left T and right T") {
    val expr = testee match {
      case (left, right) => left(_.aString) =:= right(_.bString)
    }
    SparkExpressionEvaluator.evaluate(expr).expr.toString() should be("('LEFT.aString = 'RIGHT.bString)")
  }


  test("can equal left T and const T") {
    val expr = testee match {
      case (left, _) => left(_.aString) =:= "hello"
    }
    SparkExpressionEvaluator.evaluate(expr).expr.toString() should be("('LEFT.aString = hello)")
  }

  test("can equal right T and const T") {
    val expr = testee match {
      case (_, right) => right(_.bString) =:= "hello"
    }
    SparkExpressionEvaluator.evaluate(expr).expr.toString() should be("('RIGHT.bString = hello)")
  }

  test("can form an `and` predicate") {
    val expr = testee match {
      case (left, right) => left(_.aString) =:= right(_.bString) && right(_.bString) =:= "hello!"
    }
    SparkExpressionEvaluator.evaluate(expr).expr.toString() should be(
      "(('LEFT.aString = 'RIGHT.bString) && ('RIGHT.bString = hello!))"
    )
  }

  test("left can be greater than right") {
    val expr = testee match {
      case (left, right) => left(_.aString) > right(_.bString)
    }
    SparkExpressionEvaluator.evaluate(expr).expr.toString() should be("('LEFT.aString > 'RIGHT.bString)")
  }

  test("left can be greater than or equal to right") {
    val expr = testee match {
      case (left, right) => left(_.aString) >= right(_.bString)
    }
    SparkExpressionEvaluator.evaluate(expr).expr.toString() should be("('LEFT.aString >= 'RIGHT.bString)")
  }

  test("left can be less than right") {
    val expr = testee match {
      case (left, right) => left(_.aString) < right(_.bString)
    }
    SparkExpressionEvaluator.evaluate(expr).expr.toString() should be("('LEFT.aString < 'RIGHT.bString)")
  }

  test("left can be less than or equal to right") {
    val expr = testee match {
      case (left, right) => left(_.aString) <= right(_.bString)
    }
    SparkExpressionEvaluator.evaluate(expr).expr.toString() should be("('LEFT.aString <= 'RIGHT.bString)")
  }

  test("left can be greater than const") {
    val expr = testee match {
      case (left, _) => left(_.aString) > "abc"
    }
    SparkExpressionEvaluator.evaluate(expr).expr.toString() should be("('LEFT.aString > abc)")
  }

  test("left can be greater than or equal to const") {
    val expr = testee match {
      case (left, _) => left(_.aString) >= "abc"
    }
    SparkExpressionEvaluator.evaluate(expr).expr.toString() should be("('LEFT.aString >= abc)")
  }

  test("left can be less than const") {
    val expr = testee match {
      case (left, _) => left(_.aString) < "abc"
    }
    SparkExpressionEvaluator.evaluate(expr).expr.toString() should be("('LEFT.aString < abc)")
  }

  test("left can be less than or equal to const") {
    val expr = testee match {
      case (left, _) => left(_.aString) <= "abc"
    }
    SparkExpressionEvaluator.evaluate(expr).expr.toString() should be("('LEFT.aString <= abc)")
  }

  test("right can be greater than const") {
    val expr = testee match {
      case (_, right) => right(_.bString) > "abc"
    }
    SparkExpressionEvaluator.evaluate(expr).expr.toString() should be("('RIGHT.bString > abc)")
  }

  test("right can be greater than or equal to const") {
    val expr = testee match {
      case (_, right) => right(_.bString) >= "abc"
    }
    SparkExpressionEvaluator.evaluate(expr).expr.toString() should be("('RIGHT.bString >= abc)")
  }

  test("right can be less than const") {
    val expr = testee match {
      case (_, right) => right(_.bString) < "abc"
    }
    SparkExpressionEvaluator.evaluate(expr).expr.toString() should be("('RIGHT.bString < abc)")
  }

  test("right can be less than or equal to const") {
    val expr = testee match {
      case (_, right) => right(_.bString) <= "abc"
    }
    SparkExpressionEvaluator.evaluate(expr).expr.toString() should be("('RIGHT.bString <= abc)")
  }

}
