package joinwiz.expression

import joinwiz.expression.EqualSyntax2Test.Entity
import joinwiz.{ComputationEngine, SparkSuite}
import org.apache.spark.sql.{Dataset, Encoders}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

object EqualSyntax2Test {
  case class Entity(pk: String = null, opt: Option[String] = None)
}

abstract class EqualSyntax2Test[F[_]] extends AnyFunSuite with Matchers {
  implicit val computationEngine: ComputationEngine[F]
  def entities(e: Entity*): F[Entity]

  import joinwiz.syntax._

  private val a_none  = Entity("a", None)
  private val a_someA = Entity("a", Some("a"))
  private val a_someB = Entity("a", Some("b"))
  private val b_none  = Entity("b", None)
  private val b_someA = Entity("b", Some("a"))
  private val b_someB = Entity("b", Some("b"))

  test("left T == right T") {
    val left  = entities(a_none, a_someA)
    val right = entities(a_someB, b_none)

    left
      .innerJoin(right)((l, r) => l(_.pk) =:= r(_.pk))
      .collect() should contain only (
      (a_none, a_someB),
      (a_someA, a_someB)
    )
  }

  test("left T == right Option[T]") {
    val left  = entities(a_none, a_someA)
    val right = entities(a_someB, b_someA)

    left
      .innerJoin(right)((l, r) => l(_.pk) =:= r(_.opt))
      .collect() should contain only (
      (a_none, b_someA),
      (a_someA, b_someA)
    )
  }

  test("left Option[T] == right T") {
    val left  = entities(a_none, a_someA)
    val right = entities(a_someB, b_none)

    left
      .innerJoin(right)((l, r) => l(_.opt) =:= r(_.pk))
      .collect() should contain only ((a_someA, a_someB))
  }

  test("left T == const T") {
    val left  = entities(a_none, b_none)
    val right = entities(b_none)

    left
      .innerJoin(right)((l, _) => l(_.pk) =:= "a")
      .collect() should contain only ((a_none, b_none))
  }

  test("left Option[T] == const Option[T]") {
    val left  = entities(a_someA, a_someB)
    val right = entities(b_none)

    left
      .innerJoin(right)((l, _) => l(_.pk) =:= Some("a"))
      .collect() should contain only ((a_someA, b_none))
  }

  test("right T == const T") {
    val left  = entities(a_none)
    val right = entities(a_someA, b_someA)

    left
      .innerJoin(right)((_, r) => r(_.pk) =:= Some("a"))
      .collect() should contain only ((a_someA, b_none))
  }
}

class SparkBasedEqualSyntax2Test extends EqualSyntax2Test[Dataset] with SparkSuite {
  override implicit val computationEngine: ComputationEngine[Dataset] = joinwiz.spark.sparkBasedEngine
  override def entities(a: Entity*): Dataset[Entity]                  = ss.createDataset(a)(Encoders.product[Entity])
}

class SeqBasedEqualSyntax2Test extends EqualSyntax2Test[Seq] {
  override implicit val computationEngine: ComputationEngine[Seq] = joinwiz.testkit.fakeComputationEngine
  override def entities(a: Entity*): Seq[Entity]                  = a.toSeq
}
