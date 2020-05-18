package joinwiz

import joinwiz.ComputationEngineTest._
import org.apache.spark.sql.Dataset
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.language.higherKinds

object ComputationEngineTest {
  case class Entity(uuid: Int, value: String)
}

abstract class ComputationEngineTest[F[_]: ComputationEngine] extends AnyFunSuite with Matchers {
  import joinwiz.syntax._

  def entities(a: Entity*): F[Entity]

  test("can inner join") {
    val l1 = Entity(1, "skipme-left")
    val l2 = Entity(2, "joinme-left")

    val r1 = Entity(2, "joinme-right")
    val r2 = Entity(3, "skipme-right")

    val left  = entities(l1, l2)
    val right = entities(r1, r2)

    left
      .innerJoin(right)((l, r) => l(_.uuid) =:= r(_.uuid))
      .collect() should contain only ((l2, r1))
  }

  test("can left join") {
    val l1 = Entity(1, "joinme-left-1")
    val l2 = Entity(2, "joinme-left-2")

    val r1 = Entity(2, "joinme-right")
    val r2 = Entity(3, "skipme-right")

    val left  = entities(l1, l2)
    val right = entities(r1, r2)

    left
      .leftJoin(right)((l, r) => l(_.uuid) =:= r(_.uuid))
      .collect() should contain only ((l1, null), (l2, r1))
  }

  test("can map") {
    entities(Entity(1, "* -1"), Entity(2, "* -1"))
      .map(x => x.uuid * -1)
      .collect() should contain only (-1, -2)
  }

  test("can flatMap") {
    entities(Entity(1, "a b c d"))
      .flatMap(x => x.value.split(" "))
      .collect() should contain only ("a", "b", "c", "d")
  }

  test("can filter") {
    val even = Entity(2, "pass")
    val odd  = Entity(3, "skip")

    entities(even, odd)
      .filter(x => x.uuid % 2 == 0)
      .collect() should contain only (even)
  }

  test("can distinct") {
    val e = Entity(1, "hello world")

    entities(e, e.copy(), e.copy())
      .distinct()
      .collect()
      .size should be(1)
  }

  test("can group by key: map groups") {
    val e1 = Entity(1, "hello")
    val e2 = Entity(1, "world")
    val e3 = Entity(2, "waa")
    val e4 = Entity(2, "zzz")
    val e5 = Entity(2, "up")

    entities(e1, e2, e3, e4, e5)
      .groupByKey(_.uuid)
      .mapGroups {
        case (key, entities) => key -> entities.size
      }
      .collect() should contain only (
      1 -> 2,
      2 -> 3
    )
  }

  test("can group by key: reduce groups") {
    val e1 = Entity(1, "looooooong")
    val e2 = Entity(1, "short")
    val e3 = Entity(2, "looooooong")
    val e4 = Entity(2, "shorter")
    val e5 = Entity(2, "short")

    entities(e1, e2, e3, e4, e5)
      .groupByKey(_.uuid)
      .reduceGroups {
        case (e1, e2) => if (e1.value.length > e2.value.length) e1 else e2
      }
      .collect() should contain only (
      1 -> e1,
      2 -> e3
    )
  }

  test("can union") {
    val e1 = Entity(1, "hello")
    val e2 = Entity(1, "world")
    val e3 = Entity(2, "waa")
    val e4 = Entity(2, "zzz")
    val e5 = Entity(2, "up")

    (entities(e1, e2) unionByName entities(e3, e4, e5)).collect() should contain only (e1, e2, e3, e4, e5)
  }

}
import joinwiz.spark._
class SparkComputationEngineTest extends ComputationEngineTest[Dataset] with Matchers with SparkSuite {
  import ss.implicits._

  override def entities(a: Entity*): Dataset[Entity] = a.toDS
}