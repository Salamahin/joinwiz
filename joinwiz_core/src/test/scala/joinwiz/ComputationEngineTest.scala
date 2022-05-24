package joinwiz

import joinwiz.ComputationEngineTest._
import org.apache.spark.sql.Dataset
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.language.higherKinds
import scala.reflect.runtime.universe.TypeTag

object ComputationEngineTest {
  case class Entity(uuid: Int, value: String)
  case class OtherEntity(uuid: Int, value: String)
  case class EntityWithOpt(optUuid: Option[Int])
}

abstract class ComputationEngineTest[F[_]: ComputationEngine] extends AnyFunSuite with Matchers {

  import joinwiz.syntax._

  def entities[T <: Product: TypeTag](a: T*): F[T]

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
      .collect() should contain only ((l1, None), (l2, Some(r1)))
  }

  test("can extract members from option") {
    val l1 = (Entity(1, "e1"), None: Option[Entity])
    val l2 = (Entity(2, "e2"), Some(Entity(2, "e2")))

    val r1 = Entity(2, "e3")
    val r2 = Entity(3, "e4")

    val left  = entities(l1, l2)
    val right = entities(r1, r2)

    left
      .leftJoin(right) {
        case (_ wiz maybeLeft, right) => maybeLeft((e: Entity) => e.uuid) =:= right(_.uuid)
      }
      .collect() should contain only ((l1, None), (l2, Some(r1)))
  }

  test("flattens optional value extracted from optional entity") {
    val entity       = EntityWithOpt(Some(1))
    val bothPresent  = (Some(entity), Some(entity))
    val leftPresent  = (Some(entity), None)
    val rightPresent = (None, Some(entity))

    val ds = entities(
      bothPresent,
      leftPresent,
      rightPresent
    )

    ds
      .innerJoin(ds) {
        case (_ wiz l2, _ wiz r2) => l2((e: EntityWithOpt) => e.optUuid) =:= r2((e: EntityWithOpt) => e.optUuid)
      }
      .collect() should contain only (
      (bothPresent, bothPresent),
      (bothPresent, rightPresent),
      (rightPresent, bothPresent),
      (rightPresent, rightPresent)
    )
  }

  test("can left anti join") {
    val l1 = Entity(1, "only-in-left")
    val l2 = Entity(2, "only-in-left")
    val l3 = Entity(3, "in-left-and-in-right")

    val r1 = OtherEntity(3, "in-left-and-in-right")
    val r2 = OtherEntity(4, "only-in-right")

    val left  = entities(l1, l2, l3)
    val right = entities(r1, r2)

    left
      .leftAntiJoin(right)((l, r) => l(_.uuid) =:= r(_.uuid))
      .collect() should contain only (l1, l2)
  }

  test("can map") {
    entities(Entity(1, "* -1"), Entity(2, "* -1"))
      .map(x => x.uuid * -1)
      .collect() should contain only (-1, -2)
  }

  test("can flatMap") {
    entities(Entity(1, "a b c d"))
      .flatMap(x => x.value.split(" ").toSeq)
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

  test("can group by key: count") {
    val e1 = Entity(1, "val1")
    val e2 = Entity(1, "val2")
    val e3 = Entity(2, "val3")
    val e4 = Entity(2, "val4")
    val e5 = Entity(2, "val5")

    entities(e1, e2, e3, e4, e5)
      .groupByKey(_.uuid)
      .count()
      .collect() should contain only (
      1 -> 2L,
      2 -> 3L
    )
  }

  test("can group by key: cogroup") {
    val e1 = Entity(1, "val1")
    val e2 = Entity(2, "val2")
    val e3 = Entity(2, "val3")
    val e4 = Entity(3, "val4")

    val collected = entities(e1, e2, e3)
      .groupByKey(_.uuid)
      .cogroup(entities(e2, e3, e4).groupByKey(_.uuid)) {
        case (k, it1, it2) =>
          val left  = it1.map(_.value).toSeq.sorted.mkString(",")
          val right = it2.map(_.value).toSeq.sorted.mkString(",")

          s"key=$k left=$left right=$right" :: Nil
      }
      .collect()

    collected should contain only (
      "key=1 left=val1 right=",
      "key=2 left=val2,val3 right=val2,val3",
      "key=3 left= right=val4"
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

  test("can build row_number") {
    val e1 = Entity(1, "b")
    val e2 = Entity(1, "a")
    val e3 = Entity(2, "c")
    val e4 = Entity(2, "b")
    val e5 = Entity(2, "a")

    entities(e1, e2, e3, e4, e5)
      .withWindow { window =>
        window
          .partitionBy(_.uuid)
          .orderByAsc(_.value)
          .call(row_number)
      }
      .collect() should contain only (
      (e1, 2),
      (e2, 1),
      (e3, 3),
      (e4, 2),
      (e5, 1)
    )
  }

}

import joinwiz.spark._

class SparkComputationEngineTest extends ComputationEngineTest[Dataset] with Matchers with SparkSuite {

  import ss.implicits._

  override def entities[T <: Product: TypeTag](a: T*): Dataset[T] = a.toDS()
}
