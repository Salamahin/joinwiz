package joinwiz

import joinwiz.SeqJoinImplTest.{A, B}
import joinwiz.law.AllLaws
import joinwiz.testkit.DatasetOperations
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.language.higherKinds


object SeqJoinImplTest {

  case class A(pk: String, value: String)

  case class B(fk: String, value: Option[BigDecimal])

}

class SeqJoinImplTest extends AnyFunSuite with Matchers with AllLaws with BeforeAndAfterAll {

  private val a1 = A("pk1", "val1")
  private val a2 = A("pk1", "val2")
  private val a3 = A("pk2", "val3")

  private val b1 = B("pk1", Some(BigDecimal(0)))
  private val b2 = B("pk2", None)

  private val as = Seq(a1, a2, a3)
  private val bs = Seq(b1, b2)

  private var ss: SparkSession = _
  private var aDs: Dataset[A] = _
  private var bDs: Dataset[B] = _

  override def beforeAll() {
    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .getOrCreate()

    import sparkSession.implicits._

    ss = sparkSession
    aDs = as.toDS()
    bDs = bs.toDS()
  }

  override def afterAll() {
    ss.close()
  }

  private def testMe[F[_] : DatasetOperations](ft: F[A], fu: F[B]) = {
    import joinwiz.testkit.syntax._

    ft
      .innerJoin(fu)(
        (l, r) => l(_.pk) =:= r(_.fk) && l(_.value) =:= "val1" && r(_.value) =:= Some(BigDecimal(0L))
      )
      .map {
        case (a, b) => (b, a)
      }
  }

  test("sparkless inner join") {
    import joinwiz.testkit.sparkless.implicits._
    testMe(as, bs) should contain only ((b1, a1))
  }


  test("spark's inner join") {
    import joinwiz.testkit.spark.implicits._
    testMe(aDs, bDs).collect() should contain only ((b1, a1))
  }
}
