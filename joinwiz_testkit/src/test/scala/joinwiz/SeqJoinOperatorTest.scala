package joinwiz

import joinwiz.SeqJoinOperatorTest.{A, B}
import joinwiz.law.AllLaws
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers


object SeqJoinOperatorTest {

  case class A(pk: String, value: String)

  case class B(fk: String, value: Option[BigDecimal])

}

class SeqJoinOperatorTest extends AnyFunSuite with Matchers with AllLaws with BeforeAndAfterAll {

  private val a1 = A("pk1", "val1")
  private val a2 = A("pk1", "val2")
  private val a3 = A("pk2", "val3")

  private val b1 = B("pk1", Some(BigDecimal(0)))
  private val b2 = B("pk2", None)

  private val as = Seq(a1, a2, a3)
  private val bs = Seq(b1, b2)

  private def testMe[F[_], V[_]](ft: F[A], fu: F[B])(implicit
                                                     j: JoinApi[F, A],
                                                     m: MapApi[F, V, (A, B)],
                                                     e: V[(B, A)]) = {
    DatasetApi(ft) {
      _
        .innerJoin(fu)(
          (l, r) =>
            l(_.pk) =:= r(_.fk) &&
              l(_.value) =:= "val1" &&
              r(_.value) =:= Some(BigDecimal(0L))
        )
        .map {
          case (a, b) => (b, a)
        }
    }
  }

  test("sparkless inner join") {
    import SparklessApi._
    testMe(as, bs) should contain only ((a1, b1))
  }


  test("spark's inner join") {
    val ss = SparkSession.builder()
      .master("local[*]")
      .getOrCreate()

    import SparkApi._
    import ss.implicits._

    testMe(as.toDS(), bs.toDS()).collect() should contain only ((a1, b1))

    ss.close()
  }
}
