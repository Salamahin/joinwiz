package joinwiz.window

import joinwiz.TWindow
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.WindowSpec
import org.apache.spark.sql.functions.{row_number => spark_row_number}

case class TWindowSpec[O, T](func: WindowFunction[O, T], window: TWindow[O, _])

trait WindowFunction[O, T] {
  def apply(window: WindowSpec): Column
  def apply(rows: Seq[O]): Seq[T]
}

trait WindowExpressionSyntax {
  implicit class FunctionOverWindowSyntax[O, T](wf: WindowFunction[O, T]) {
    def over[E](tw: TWindow[O, E]): TWindowSpec[O, T] = TWindowSpec[O, T](wf, tw)
  }
}

trait CommonWindowFunctions {
  def row_number[O]: WindowFunction[O, Int] = new WindowFunction[O, Int] {
    override def apply(window: WindowSpec): Column = spark_row_number() over window
    override def apply(rows: Seq[O]): Seq[Int]     = rows.indices.map(_ + 1)
  }
}
