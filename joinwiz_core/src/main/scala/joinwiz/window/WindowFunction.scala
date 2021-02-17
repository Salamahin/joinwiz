package joinwiz.window

import joinwiz.TWindow
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.WindowSpec
import org.apache.spark.sql.functions.{row_number => spark_row_number}

case class TWindowSpec[O, T](func: WindowFunction[T], window: TWindow[O, _])

trait WindowFunction[T] {
  def apply(window: WindowSpec): Column
  def apply[O](rows: Seq[O]): Seq[T]
}

trait WindowExpressionSyntax {
  implicit class FunctionOverWindowSyntax[O, T](wf: WindowFunction[T]) {
    def over[E](tw: TWindow[O, E]): TWindowSpec[O, T] = TWindowSpec[O, T](wf, tw)
  }
}

trait CommonWindowFunctions {
  def row_number: WindowFunction[Int] = new WindowFunction[Int] {
    override def apply(window: WindowSpec): Column = spark_row_number() over window
    override def apply[O](rows: Seq[O]): Seq[Int]  = rows.indices.map(_ + 1)
  }
}
