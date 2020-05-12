package joinwiz

import joinwiz.DatasetSyntaxTest._
import org.apache.spark.sql.Dataset
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.language.higherKinds

object DatasetSyntaxTest {
  case class A(uuid: Int, value: String)
  case class B(uuid: Int, value: String)
}

trait DatasetSyntaxTest {
  private val a1 = A(1, "unique a")
  private val a2 = A(2, "duplicated a")
  private val a3 = A(2, "duplicated a")

  private val b1 = B(1, "duplicated value of b")
  private val b2 = B(2, "duplicated value of b")
  private val b3 = B(2, "unique value of b")

  val as = Seq(a1, a2, a3)
  val bs = Seq(b1, b2, b3)

  protected val expected = Seq("duplicated", "unique")

  def testee[F[_]: DatasetOperations](as: F[A], bs: F[B]) = {
    import joinwiz.syntax._

    as.distinct()
      //a1, a2
      .innerJoin(bs) { (l, r) =>
        l(_.uuid) =:= r(_.uuid)
      }
      //(a1, b1)
      //(a2, b2)
      //(a2, b3)
      .groupByKey {
        case (_, b) => b.value
      }
      //duplicated value of b, it((a1, b1), (a2, b2))
      //unique value of b, it(a2, b3)
      .mapGroups {
        case (key, _) => key
      }
      //duplicated value of b
      //unique value of b
      .flatMap(_.split(" "))
      .filter(_.length > 5)
    //duplicated
    //unique
  }
}

class SparkDatasetSyntaxTest extends AnyFunSuite with DatasetSyntaxTest with Matchers with SparkSuite {
  private var aDs: Dataset[A] = _
  private var bDs: Dataset[B] = _

  override def beforeAll() {
    super.beforeAll()

    val spark = ss
    import spark.implicits._

    aDs = as.toDS()
    bDs = bs.toDS()
  }

  test("has dataset-like syntax") {
    import joinwiz.spark.implicits._
    testee(aDs, bDs).collect() should contain only ("duplicated", "unique")
  }
}
