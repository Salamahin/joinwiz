package joinwiz.expression

import joinwiz.{LTColumn, TColumn, TColumnSyntax}

case class A(b: Option[B])
case class B(c: Option[C])
case class C(value: Int)

object Aaaa extends App with TColumnSyntax {
  val a = TColumn.left[A]
  private val value = a >> (_.b) >> (_.c)
  println(value)
}
