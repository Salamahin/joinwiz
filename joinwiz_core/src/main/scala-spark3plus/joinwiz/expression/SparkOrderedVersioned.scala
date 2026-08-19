package joinwiz.expression

import java.time.{Instant, LocalDate}

trait SparkOrderedVersioned extends SparkOrderedInstances {
  private implicit val localDateOrdering: Ordering[LocalDate] = new Ordering[LocalDate] {
    override def compare(x: LocalDate, y: LocalDate): Int = x.compareTo(y)
  }
  private implicit val instantOrdering: Ordering[Instant] = new Ordering[Instant] {
    override def compare(x: Instant, y: Instant): Int = x.compareTo(y)
  }

  implicit val localDateOrdered: SparkOrdered[LocalDate] = of
  implicit val instantOrdered: SparkOrdered[Instant]     = of
}
