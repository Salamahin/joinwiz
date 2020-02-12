package joinwiz.testkit.template

import joinwiz.testkit.template.InstanceCreatorTest.A
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

object InstanceCreatorTest {
  case class B(str: String, optStr: Option[String], seqStrs: Seq[String])
  case class A(a: Boolean)
}

class InstanceCreatorTest extends AnyFunSuite with Matchers {
  test("can create an instance using template") {
    val template = new InstanceCreator with ZeroTemplates
    import template._

    template[A]
  }
}
