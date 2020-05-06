package joinwiz.custom

import joinwiz._
import joinwiz.syntax.JOIN_CONDITION
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, Encoder, Encoders}

import scala.language.postfixOps

// Named in honor of our former TeamLead, who taught us how to deal with skewed data
object KhomutovJoin {

  private def nullableField(e: Expression): Option[String] = e match {
    case Equality(left: LeftTypedColumn[_, _, _], _: RightTypedColumn[_, _, _]) => Some(left.name)
    case _                                                       => None
  }

  implicit class KhomutovJoinSyntax[T: Encoder](ds: Dataset[T]) {
    def khomutovJoin[U: Encoder](other: Dataset[U])(joinBy: JOIN_CONDITION[T, U]): Dataset[(T, U)] = {
      val operator = joinBy(ApplyToLeftColumn[T], ApplyToRightColumn[U])
      val nullableFieldName = KhomutovJoin
        .nullableField(operator)
        .getOrElse(throw new UnsupportedOperationException(s"Expression $operator is not supported yet"))

      val dsWithoutNulls = ds.filter(col(nullableFieldName) isNotNull)
      val dsWithNulls    = ds.filter(col(nullableFieldName) isNull)

      implicit val tuEnc: Encoder[(T, U)] = Encoders.tuple(implicitly[Encoder[T]], implicitly[Encoder[U]])

      import joinwiz.spark.implicits._
      import joinwiz.syntax._

      dsWithoutNulls
        .leftJoin(other)(joinBy)
        .unionByName(dsWithNulls.map((_, null.asInstanceOf[U])).map(identity))
    }
  }

}
