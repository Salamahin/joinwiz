package joinwiz

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.{Instant, LocalDate}

object TemporalCompareTest {
  case class Event(id: Int, day: LocalDate, at: Instant)
}

class TemporalCompareTest extends AnyFunSuite with Matchers with SparkSuite {
  import TemporalCompareTest._
  import joinwiz.syntax._
  import joinwiz.spark._
  import ss.implicits._

  private val d0 = LocalDate.parse("2020-01-01")
  private val d1 = LocalDate.parse("2020-06-01")
  private val events = Seq(
    Event(1, d0, Instant.parse("2020-01-01T00:00:00Z")),
    Event(2, d1, Instant.parse("2020-06-01T00:00:00Z"))
  )

  test("LocalDate and Instant are comparable in the DSL") {
    assertCompiles("FColumn[Event](_.day) < d1")
    assertCompiles("FColumn[Event](_.at) >= Instant.now()")
    assertDoesNotCompile("FColumn[Event](_.id) < d1")
  }

  test("filterBy on a LocalDate column round-trips through Spark") {
    events.toDS().filterBy(e => e(_.day) < d1).collect().toSeq should
      contain theSameElementsAs events.filter(_.day.isBefore(d1))
  }
}
