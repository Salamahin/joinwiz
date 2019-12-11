package joinwiz.custom

import joinwiz.JoinWiz.JOIN_CONDITION
import joinwiz._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col

import scala.language.postfixOps

object KhomutovJoin {

  private def nullableField(e: Operator): Option[LeftField] = e match {
    case Equality(left: LeftField, _: RightField) => Some(left)
    case _ => None
  }

  implicit class DatasetSyntax[T](ds: Dataset[T]) {
    def khomutovJoin[U](other: Dataset[U])(joinBy: JOIN_CONDITION[T, U]): Dataset[(T, U)] = {
      val operator = joinBy(new LTColumnExtractor[T], new RTColumnExtractor[U])
      val nullableField = KhomutovJoin
        .nullableField(operator)
        .getOrElse(throw new UnsupportedOperationException(s"Expression $operator is not supported yet"))

      val dsWithoutNulls = ds.filter(col(nullableField.name) isNotNull)
      val dsWithNulls = ds.filter(col(nullableField.name) isNull)

      import JoinWiz._
      import ds.sparkSession.implicits._

      dsWithoutNulls
        .joinWiz(other, "left_outer")(joinBy)
        .unionByName(dsWithNulls.map((_, null.asInstanceOf[U])).map(identity))
    }
  }

}
