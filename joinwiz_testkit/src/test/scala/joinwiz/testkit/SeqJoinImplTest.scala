//package joinwiz.testkit
//
//import joinwiz.testkit.SeqJoinImplTest.{A, B}
//import joinwiz.{DatasetOperations, SparkSuite}
//import org.apache.spark.sql.Dataset
//import org.scalatest.funsuite.AnyFunSuite
//import org.scalatest.matchers.should.Matchers
//
//import scala.language.higherKinds
//
//object SeqJoinImplTest {
//  case class A(pk: String)
//  case class B(fk: String)
//}
//
//class SeqJoinImplTest extends AnyFunSuite with Matchers with SparkSuite {
//
//  private val a1 = A("pk1")
//  private val a2 = A("pk2")
//  private val a3 = A("pk3")
//
//  private val b1 = B("pk1")
//  private val b2 = B("pk2")
//
//  private val as = Seq(a1, a2, a3)
//  private val bs = Seq(b1, b2)
//
//  private var aDs: Dataset[A] = _
//  private var bDs: Dataset[B] = _
//
//  override def beforeAll() {
//    super.beforeAll()
//
//    val spark = ss
//    import spark.implicits._
//
//    aDs = as.toDS()
//    bDs = bs.toDS()
//  }
//
//  private def testMeInnerJoin[F[_]: DatasetOperations](fa: F[A], fb: F[B]) = {
//    import joinwiz.syntax._
//
//    fa.innerJoin(fb)(
//      (l, r) => l(_.pk) =:= r(_.fk)
//    )
//  }
//
//  private def testMeLeftJoin[F[_]: DatasetOperations](fa: F[A], fb: F[B]) = {
//    import joinwiz.syntax._
//
//    fa.leftJoin(fb)(
//      (l, r) => l(_.pk) =:= r(_.fk)
//    )
//  }
//
//  test("sparkless inner join") {
//    import joinwiz.testkit.implicits._
//    testMeInnerJoin(as, bs) should contain only (
//      (a1, b1),
//      (a2, b2)
//    )
//  }
//
//  test("spark's inner join") {
//    import joinwiz.spark.implicits._
//    testMeInnerJoin(aDs, bDs).collect() should contain only (
//      (a1, b1),
//      (a2, b2)
//    )
//  }
//
//  test("sparkless left join") {
//    import joinwiz.testkit.implicits._
//    testMeLeftJoin(as, bs) should contain only (
//      (a1, b1),
//      (a2, b2),
//      (a3, null)
//    )
//  }
//
//  test("spark's left join") {
//    import joinwiz.spark.implicits._
//    testMeLeftJoin(aDs, bDs).collect() should contain only (
//      (a1, b1),
//      (a2, b2),
//      (a3, null)
//    )
//  }
//}
