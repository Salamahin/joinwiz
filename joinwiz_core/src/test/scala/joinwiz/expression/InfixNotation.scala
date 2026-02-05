package joinwiz.expression

import org.scalatest.matchers.{MatchResult, Matcher}

import scala.util.matching.Regex

trait InfixNotation {
  private val innerPrefix: Regex = """(>=|<=|>|<|=)\(([^()]+),\s*([^()]+)\)""".r
  private val outerPrefix: Regex = """(>=|<=|>|<|=|AND|OR)\((\([^()]+\)),\s*(\([^()]+\))\)""".r
  private val infixOp: Regex     = """\(.*\s+(>=|<=|>|<|=|AND|OR)\s+.*\)""".r

  def toInfixNotation(s: String): String = {
    var result = innerPrefix.replaceAllIn(s, m =>
      Regex.quoteReplacement(s"(${m.group(2).trim} ${m.group(1)} ${m.group(3).trim})")
    )
    result = outerPrefix.replaceAllIn(result, m =>
      Regex.quoteReplacement(s"(${m.group(2).trim} ${m.group(1)} ${m.group(3).trim})")
    )
    if (infixOp.findFirstIn(result).isEmpty)
      throw new IllegalArgumentException(s"""Unknown expression format: "$s"""")
    result
  }

  def equalInInfix(expected: String): Matcher[String] = new Matcher[String] {
    def apply(left: String): MatchResult = {
      val normalized = toInfixNotation(left)
      MatchResult(
        normalized == expected,
        s""""$normalized" (normalized from "$left") was not equal to "$expected"""",
        s""""$normalized" was equal to "$expected""""
      )
    }
  }
}
