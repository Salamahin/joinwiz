package joinwiz.expression

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class InfixNotationTest extends AnyFunSuite with Matchers with InfixNotation {

  test("unknown expression format throws") {
    the[IllegalArgumentException] thrownBy {
      toInfixNotation("gibberish")
    } should have message """Unknown expression format: "gibberish""""
  }

  test("infix notation is left unchanged") {
    toInfixNotation("(LEFT.pk = RIGHT.pk)") should be("(LEFT.pk = RIGHT.pk)")
  }

  test("prefix = is converted to infix") {
    toInfixNotation("=(LEFT.pk, RIGHT.pk)") should be("(LEFT.pk = RIGHT.pk)")
  }

  test("prefix < is converted to infix") {
    toInfixNotation("<(LEFT.pk, RIGHT.pk)") should be("(LEFT.pk < RIGHT.pk)")
  }

  test("prefix <= is converted to infix") {
    toInfixNotation("<=(LEFT.pk, RIGHT.pk)") should be("(LEFT.pk <= RIGHT.pk)")
  }

  test("prefix > is converted to infix") {
    toInfixNotation(">(LEFT.pk, RIGHT.pk)") should be("(LEFT.pk > RIGHT.pk)")
  }

  test("prefix >= is converted to infix") {
    toInfixNotation(">=(LEFT.pk, RIGHT.pk)") should be("(LEFT.pk >= RIGHT.pk)")
  }

  test("prefix with constant operand") {
    toInfixNotation("<(LEFT.pk, 1)") should be("(LEFT.pk < 1)")
  }

  test("nested prefix is converted to infix") {
    toInfixNotation("AND(=(LEFT.pk, RIGHT.pk), <(LEFT.x, RIGHT.y))") should be(
      "((LEFT.pk = RIGHT.pk) AND (LEFT.x < RIGHT.y))"
    )
  }

  test("SQL single quotes around string literals are stripped") {
    toInfixNotation("(LEFT.pk = 'matching')") should be("(LEFT.pk = matching)")
  }

  test("prefix with SQL-quoted string literal") {
    toInfixNotation("=(LEFT.pk, 'matching')") should be("(LEFT.pk = matching)")
  }

  test("prefix with dotted column paths") {
    toInfixNotation("=(LEFT._1._1.aString, RIGHT.dString)") should be(
      "(LEFT._1._1.aString = RIGHT.dString)"
    )
  }
}
