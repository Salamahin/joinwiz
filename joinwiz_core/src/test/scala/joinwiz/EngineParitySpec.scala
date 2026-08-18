package joinwiz

import joinwiz.EngineParitySpec._
import org.apache.spark.sql.Dataset
import org.scalacheck.Gen
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import scala.language.higherKinds

/**
  * Property-based parity check between the two `ComputationEngine` interpreters.
  *
  * joinwiz's whole promise is that one `[F[_]: ComputationEngine]` program runs identically
  * with Spark (`Dataset`) and without it (`Seq`, the testkit). The hand-written examples in
  * [[ComputationEngineTest]] pin a few concrete inputs; this suite instead throws randomized
  * data at the *same* generic program through both interpreters and asserts the results match.
  * That is exactly the invariant most likely to silently rot (Seq semantics drifting away from
  * Spark's — ordering, option flattening, anti-join, grouping), so it deserves generative
  * coverage rather than a couple of fixtures.
  */
object EngineParitySpec {
  case class L(uuid: Int, value: String)
  case class R(uuid: Int, tag: String)

  // The programs below are written once, generically over the engine — the library's raison
  // d'être. Each is executed against both `Dataset` and `Seq` in the tests.
  object programs {
    import joinwiz.syntax._

    def innerJoin[F[_]: ComputationEngine](ls: F[L], rs: F[R]): F[(L, R)] =
      ls.innerJoin(rs)((l, r) => l(_.uuid) =:= r(_.uuid))

    def leftJoin[F[_]: ComputationEngine](ls: F[L], rs: F[R]): F[(L, Option[R])] =
      ls.leftJoin(rs)((l, r) => l(_.uuid) =:= r(_.uuid))

    def leftAntiJoin[F[_]: ComputationEngine](ls: F[L], rs: F[R]): F[L] =
      ls.leftAntiJoin(rs)((l, r) => l(_.uuid) =:= r(_.uuid))

    def filterThenMap[F[_]: ComputationEngine](ls: F[L]): F[Int] =
      ls.filter(_.uuid % 2 == 0).map(_.uuid)

    def distinctUuids[F[_]: ComputationEngine](ls: F[L]): F[Int] =
      ls.map(_.uuid).distinct()

    def countByKey[F[_]: ComputationEngine](ls: F[L]): F[(Int, Long)] =
      ls.groupByKey(_.uuid).count()
  }
}

class EngineParitySpec extends AnyFunSuite with Matchers with ScalaCheckDrivenPropertyChecks with SparkSuite {

  import joinwiz.syntax._
  import joinwiz.spark._
  import joinwiz.testkit._
  import ss.implicits._

  // Spark spins up per property evaluation, so keep the sample count and collection sizes
  // modest — enough to exercise matches, misses and duplicates without turning CI glacial.
  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 8, sizeRange = 8)

  // uuid drawn from a small domain so random left/right rows actually join often.
  private val genL: Gen[L] = for {
    uuid  <- Gen.choose(0, 3)
    value <- Gen.alphaLowerStr.map(_.take(3))
  } yield L(uuid, value)

  private val genR: Gen[R] = for {
    uuid <- Gen.choose(0, 3)
    tag  <- Gen.alphaLowerStr.map(_.take(3))
  } yield R(uuid, tag)

  private val genLs: Gen[Seq[L]] = Gen.listOf(genL)
  private val genRs: Gen[Seq[R]] = Gen.listOf(genR)

  private def sparkThenSeq[T](ls: Seq[L], rs: Seq[R])(
    onDataset: (Dataset[L], Dataset[R]) => Dataset[T]
  )(onSeq: (Seq[L], Seq[R]) => Seq[T]): Unit = {
    val fromSpark = onDataset(ls.toDS(), rs.toDS()).collect()
    val fromSeq   = onSeq(ls, rs)

    fromSpark should contain theSameElementsAs fromSeq
  }

  test("inner join is identical on Spark and Seq") {
    forAll(genLs, genRs) { (ls, rs) => sparkThenSeq(ls, rs)(programs.innerJoin(_, _))(programs.innerJoin(_, _)) }
  }

  test("left join is identical on Spark and Seq") {
    forAll(genLs, genRs) { (ls, rs) => sparkThenSeq(ls, rs)(programs.leftJoin(_, _))(programs.leftJoin(_, _)) }
  }

  test("left anti join is identical on Spark and Seq") {
    forAll(genLs, genRs) { (ls, rs) => sparkThenSeq(ls, rs)(programs.leftAntiJoin(_, _))(programs.leftAntiJoin(_, _)) }
  }

  test("filter then map is identical on Spark and Seq") {
    forAll(genLs) { ls => sparkThenSeq(ls, Seq.empty[R])((l, _) => programs.filterThenMap(l))((l, _) => programs.filterThenMap(l)) }
  }

  test("distinct is identical on Spark and Seq") {
    forAll(genLs) { ls => sparkThenSeq(ls, Seq.empty[R])((l, _) => programs.distinctUuids(l))((l, _) => programs.distinctUuids(l)) }
  }

  test("group-by-key count is identical on Spark and Seq") {
    forAll(genLs) { ls => sparkThenSeq(ls, Seq.empty[R])((l, _) => programs.countByKey(l))((l, _) => programs.countByKey(l)) }
  }
}
