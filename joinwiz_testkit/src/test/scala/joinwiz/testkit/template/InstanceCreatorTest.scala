package joinwiz.testkit.template

import joinwiz.testkit.template.InstanceCreatorTest.Outer
import org.scalatest.funsuite.AnyFunSuite

object InstanceCreatorTest {
  case class Inner(str: String, optStr: Option[String], seqStrs: Seq[String])
  case class Outer(a: Inner)
}

class InstanceCreatorTest extends AnyFunSuite {
  test("can create an instance with nested subinstancies using template") {
    val template = new InstanceCreator with ZeroTemplates
    import template._

    template[Outer]
  }
}
