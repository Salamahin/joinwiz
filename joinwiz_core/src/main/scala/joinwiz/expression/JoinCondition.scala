package joinwiz.expression

import org.apache.spark.sql.Column

trait JoinCondition[L, R] {
  def apply(): Column
  def apply(left: L, right: R): Boolean
}

object JoinCondition {
  def joinCondition[L, R](f: (L, R) => Boolean)(c: Column): JoinCondition[L, R] = new JoinCondition[L, R] {
    override def apply(): Column                   = c
    override def apply(left: L, right: R): Boolean = f(left, right)
  }
}
