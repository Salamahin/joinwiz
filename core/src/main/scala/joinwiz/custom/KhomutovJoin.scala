package joinwiz.custom

import joinwiz.JoinWiz.JOIN_CONDITION
import joinwiz._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, Encoder, Encoders}

import scala.language.postfixOps

// Named in honor of our former TeamLead, who taught us how to deal with skewed data
object KhomutovJoin {

  private def nullableField(e: Operator): Option[LeftField] = e match {
    case Equality(left: LeftField, _: RightField) => Some(left)
    case _ => None
  }

  implicit class DatasetSyntax[T: Encoder](ds: Dataset[T]) {
    def khomutovJoin[U: Encoder](other: Dataset[U])(joinBy: JOIN_CONDITION[T, U]): Dataset[(T, U)] = {
      val operator = joinBy(new LTColumnExtractor[T], new RTColumnExtractor[U])
      val nullableField = KhomutovJoin
        .nullableField(operator)
        .getOrElse(throw new UnsupportedOperationException(s"Expression $operator is not supported yet"))

      val dsWithoutNulls = ds.filter(col(nullableField.name) isNotNull)
      val dsWithNulls = ds.filter(col(nullableField.name) isNull)

      implicit val tuEnc: Encoder[(T, U)] = Encoders.tuple(implicitly[Encoder[T]], implicitly[Encoder[U]])

      new JoinWiz.DatasetSyntax[T](dsWithoutNulls)
        .leftJoin(other)(joinBy)
        .unionByName(dsWithNulls.map((_, null.asInstanceOf[U])).map(identity))
    }
  }

}
