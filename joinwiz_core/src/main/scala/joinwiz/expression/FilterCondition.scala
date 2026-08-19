package joinwiz.expression

import org.apache.spark.sql.Column

trait FilterCondition[T] {
  def apply(): Column
  def apply(t: T): Boolean
}

object FilterCondition {
  def filterCondition[T](f: T => Boolean)(c: Column): FilterCondition[T] = new FilterCondition[T] {
    override def apply(): Column          = c
    override def apply(t: T): Boolean      = f(t)
  }
}
