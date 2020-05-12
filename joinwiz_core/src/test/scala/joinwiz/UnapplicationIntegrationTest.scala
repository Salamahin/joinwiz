package joinwiz

import joinwiz.UnapplicationIntegrationTest.{A, B, C}
import org.apache.spark.sql.Dataset
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

object UnapplicationIntegrationTest {
  case class A(a: String)
  case class B(b: String)
  case class C(c: String)
}

class UnapplicationIntegrationTest extends AnyFunSuite with Matchers with SparkSuite {
  private val a1 = A("v1")
  private val a2 = A("v2")
  private val a3 = A("v3")

  private val b1 = B("v1")
  private val b2 = B("v2")
  private val b3 = B("v3")

  private val c1 = C("v1")
  private val c2 = C("v2")
  private val c3 = C("v3")

  private val as = Seq(a1, a2, a3)
  private val bs = Seq(b1, b2, b3)
  private val cs = Seq(c1, c2, c3)

  private var aDs: Dataset[A] = _
  private var bDs: Dataset[B] = _
  private var cDs: Dataset[C] = _

  override def beforeAll() {
    super.beforeAll()

    val spark = ss
    import spark.implicits._

    aDs = as.toDS()
    bDs = bs.toDS()
    cDs = cs.toDS()
  }

  test("can join without unapplying") {
    import joinwiz.spark.implicits._
    import joinwiz.syntax._

    aDs
      .innerJoin(bDs)((l, r) => l(_.a) =:= r(_.b))
      .innerJoin(cDs)((l, r) => l(_._1.a) =:= r(_.c))
      .collect() should contain only (((a1, b1), c1), ((a2, b2), c2), ((a3, b3), c3))
  }

  test("can join unapplying left") {
    import joinwiz.spark.implicits._
    import joinwiz.syntax._

    aDs
      .innerJoin(bDs)((l, r) => l(_.a) =:= r(_.b))
      .innerJoin(cDs) {
        case (_ joined b, c) => b(_.b) =:= c(_.c)
      }
      .collect() should contain only (((a1, b1), c1), ((a2, b2), c2), ((a3, b3), c3))
  }

  test("can join unapplying right") {
    import joinwiz.spark.implicits._
    import joinwiz.syntax._

    val abDs = aDs
      .innerJoin(bDs)((l, r) => l(_.a) =:= r(_.b))

    cDs
      .innerJoin(abDs) {
        case (c, a joined _) => c(_.c) =:= a(_.a)
      }
      .collect() should contain only ((c1, (a1, b1)), (c2, (a2, b2)), (c3, (a3, b3)))
  }
}
