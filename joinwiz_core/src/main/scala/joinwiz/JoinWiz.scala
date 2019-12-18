package joinwiz

import joinwiz.law.{AndLaws, EqualityLaws}
import org.apache.spark.sql.Dataset

import scala.language.{implicitConversions, postfixOps}

object JoinWiz extends EqualityLaws with AndLaws {
  val LEFT_DS_ALIAS = "left"
  val RIGHT_DS_ALIAS = "right"

  type JOIN_CONDITION[T, U] = (LTColumnExtractor[T], RTColumnExtractor[U]) => Operator

  implicit class JoinWizSyntax[T](ds: Dataset[T]) {
    def joinWiz[U](other: Dataset[U], joinType: String)(joinBy: JOIN_CONDITION[T, U]): Dataset[(T, U)] = {
      ds
        .as(LEFT_DS_ALIAS)
        .joinWith(
          other.as(RIGHT_DS_ALIAS),
          Operator.evaluate(joinBy(new LTColumnExtractor[T], new RTColumnExtractor[U])),
          joinType
        )
    }

    def leftJoin[U](other: Dataset[U])(joinBy: JOIN_CONDITION[T, U]): Dataset[(T, U)] =
      ds.joinWiz(other, "left_outer")(joinBy)

    def innerJoin[U](other: Dataset[U])(joinBy: JOIN_CONDITION[T, U]): Dataset[(T, U)] =
      ds.joinWiz(other, "inner")(joinBy)
  }

}