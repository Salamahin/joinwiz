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

  private val testee = (ApplyToLeftColumn[A], ApplyToRightColumn[B])

  import joinwiz.syntax._

  test("can equal left T and right T") {
    val expr = testee match {
      case (left, right) => left(_.aString) =:= right(_.bString)
    }
    new SparkExpressionEvaluator().evaluate(expr).expr.toString() should be("('left.aString = 'right.bString)")
  }

  test("can equal right T and left T") {
    val expr = testee match {
      case (left, right) => right(_.bString) =:= left(_.aString)
    }
    new SparkExpressionEvaluator().evaluate(expr).expr.toString() should be("('left.aString = 'right.bString)")
  }

  test("can equal left T and right Option[T] after left being lifted to some") {
    val expr = testee match {
      case (left, right) => left(_.aString).some =:= right(_.bOptString)
    }
    new SparkExpressionEvaluator().evaluate(expr).expr.toString() should be("('left.aString = 'right.bOptString)")
  }

  test("can equal left T and right Option[T] after right being lifted to some") {
    val expr = testee match {
      case (left, right) => left(_.aOptString) =:= right(_.bString).some
    }
    new SparkExpressionEvaluator().evaluate(expr).expr.toString() should be("('left.aOptString = 'right.bString)")
  }

  test("can equal left T and const T") {
    val expr = testee match {
      case (left, _) => left(_.aString) =:= "hello"
    }
    new SparkExpressionEvaluator().evaluate(expr).expr.toString() should be("('left.aString = hello)")
  }

  test("can equal right T and const T") {
    val expr = testee match {
      case (_, right) => right(_.bString) =:= "hello"
    }
    new SparkExpressionEvaluator().evaluate(expr).expr.toString() should be("('right.bString = hello)")
  }

  test("can form an `and` predicate") {
    val expr = testee match {
      case (left, right) => left(_.aString) =:= right(_.bString) && right(_.bString) =:= left(_.aString)
    }
    new SparkExpressionEvaluator().evaluate(expr).expr.toString() should be(
      "(('left.aString = 'right.bString) && ('left.aString = 'right.bString))"
    )
  }

  test("left can be greater than right") {
    val expr = testee match {
      case (left, right) => left(_.aString) > right(_.bString)
    }
    new SparkExpressionEvaluator().evaluate(expr).expr.toString() should be("('left.aString > 'right.bString)")
  }

  test("left can be greater than or equal to right") {
    val expr = testee match {
      case (left, right) => left(_.aString) >= right(_.bString)
    }
    new SparkExpressionEvaluator().evaluate(expr).expr.toString() should be("('left.aString >= 'right.bString)")
  }

  test("left can be less than right") {
    val expr = testee match {
      case (left, right) => left(_.aString) < right(_.bString)
    }
    new SparkExpressionEvaluator().evaluate(expr).expr.toString() should be("('left.aString < 'right.bString)")
  }

  test("left can be less than or equal to right") {
    val expr = testee match {
      case (left, right) => left(_.aString) <= right(_.bString)
    }
    new SparkExpressionEvaluator().evaluate(expr).expr.toString() should be("('left.aString <= 'right.bString)")
  }

  test("left can be greater than const") {
    val expr = testee match {
      case (left, _) => left(_.aString) > "abc"
    }
    new SparkExpressionEvaluator().evaluate(expr).expr.toString() should be("('left.aString > abc)")
  }

  test("left can be greater than or equal to const") {
    val expr = testee match {
      case (left, _) => left(_.aString) >= "abc"
    }
    new SparkExpressionEvaluator().evaluate(expr).expr.toString() should be("('left.aString >= abc)")
  }

  test("left can be less than const") {
    val expr = testee match {
      case (left, _) => left(_.aString) < "abc"
    }
    new SparkExpressionEvaluator().evaluate(expr).expr.toString() should be("('left.aString < abc)")
  }

  test("left can be less than or equal to const") {
    val expr = testee match {
      case (left, _) => left(_.aString) <= "abc"
    }
    new SparkExpressionEvaluator().evaluate(expr).expr.toString() should be("('left.aString <= abc)")
  }

  test("right can be greater than const") {
    val expr = testee match {
      case (_, right) => right(_.bString) > "abc"
    }
    new SparkExpressionEvaluator().evaluate(expr).expr.toString() should be("('right.bString > abc)")
  }

  test("right can be greater than or equal to const") {
    val expr = testee match {
      case (_, right) => right(_.bString) >= "abc"
    }
    new SparkExpressionEvaluator().evaluate(expr).expr.toString() should be("('right.bString >= abc)")
  }

  test("right can be less than const") {
    val expr = testee match {
      case (_, right) => right(_.bString) < "abc"
    }
    new SparkExpressionEvaluator().evaluate(expr).expr.toString() should be("('right.bString < abc)")
  }

  test("right can be less than or equal to const") {
    val expr = testee match {
      case (_, right) => right(_.bString) <= "abc"
    }
    new SparkExpressionEvaluator().evaluate(expr).expr.toString() should be("('right.bString <= abc)")
  }

}
