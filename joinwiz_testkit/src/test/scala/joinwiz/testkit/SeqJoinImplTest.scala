package joinwiz.testkit

import joinwiz.testkit.SeqJoinImplTest.{A, B}
import joinwiz.{DatasetOperations, SparkSuite}
import org.apache.spark.sql.Dataset
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.language.higherKinds

object SeqJoinImplTest {

  case class A(pk: String, value: String)

  case class B(fk: String, value: Option[BigDecimal])

}

class SeqJoinImplTest extends AnyFunSuite with Matchers with SparkSuite {

  private val a1 = A("pk1", "val1")
  private val a2 = A("pk1", "val2")
  private val a3 = A("pk2", "val3")

  private val b1 = B("pk1", Some(BigDecimal(0)))
  private val b2 = B("pk2", None)

  private val as = Seq(a1, a2, a3)
  private val bs = Seq(b1, b2)

  private var aDs: Dataset[A] = _
  private var bDs: Dataset[B] = _

  override def beforeAll() {
    super.beforeAll()

    val spark = ss
    import spark.implicits._

    aDs = as.toDS()
    bDs = bs.toDS()
  }

  private def testMe[F[_]: DatasetOperations](fa: F[A], fb: F[B]) = {
    import joinwiz.syntax._

    fa.innerJoin(fb)(
        (l, r) => l(_.pk) =:= r(_.fk) && l(_.value) =:= "val1" && r(_.value) =:= Some(BigDecimal(0L))
      )
      .map {
        case (a, b) => (b, a)
      }
  }

  test("sparkless inner join") {
    import joinwiz.testkit.implicits._
    testMe(as, bs) should contain only ((b1, a1))
  }

  test("spark's inner join") {
    import joinwiz.spark.implicits._
    testMe(aDs, bDs).collect() should contain only ((b1, a1))
  }
}