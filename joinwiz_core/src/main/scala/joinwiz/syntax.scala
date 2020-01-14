package joinwiz

import joinwiz.law._
import org.apache.spark.sql.Dataset

import scala.language.{implicitConversions, postfixOps}

object syntax extends AllLaws {
  type JOIN_CONDITION[T, U] = (LTColumnExtractor[T, T], RTColumnExtractor[U]) => Operator
  val LEFT_DS_ALIAS = "left"
  val RIGHT_DS_ALIAS = "right"

  implicit class JoinWizSyntax[T](ds: Dataset[T]) {
    def leftJoin[U](other: Dataset[U])(joinBy: JOIN_CONDITION[T, U]): Dataset[(T, U)] =
      ds.joinWiz(other, "left_outer")(joinBy)

    def innerJoin[U](other: Dataset[U])(joinBy: JOIN_CONDITION[T, U]): Dataset[(T, U)] =
      ds.joinWiz(other, "inner")(joinBy)

    def joinWiz[U](other: Dataset[U], joinType: String)(joinBy: JOIN_CONDITION[T, U]): Dataset[(T, U)] = {
      ds
        .as(LEFT_DS_ALIAS)
        .joinWith(
          other.as(RIGHT_DS_ALIAS),
          new ColumnEvaluator().evaluate(joinBy(LTColumnExtractor[T], new RTColumnExtractor[U])),
          joinType
        )
    }
  }

}
