package joinwiz

import joinwiz.MappingTest._
import joinwiz.spark.SparkExpressionEvaluator
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

object MappingTest {
  case class A(pk: String)
  case class B(fk: Option[String])
}

class MappingTest extends AnyFunSuite with SparkSuite with Matchers {
  import joinwiz.syntax._

  test("expression when lifting left to option") {
    val expr = (ApplyLeft[A], ApplyRight[A]) match {
      case (left, right) => left(_.pk).map(_ + "!") =:= right(_.pk)
    }
    SparkExpressionEvaluator.evaluate(expr).expr.toString() should be("(UDF('LEFT.pk) = 'RIGHT.pk)")
  }

  test("can join by option") {
    import joinwiz.spark._
    import joinwiz.syntax._
    import ss.implicits._

    val a = A("pk")
    val b = B(Some("pk"))

    val as = ss.createDataset(List(a))
    val bs = ss.createDataset(List(b))


    as.innerJoin(bs)((l, r) => l(_.pk).some =:= r(_.fk))
      .collect() should contain only ((a, b))

    bs.innerJoin(as)((l, r) => l(_.fk) =:= r(_.pk).some)
      .collect() should contain only ((b, a))
  }
}
