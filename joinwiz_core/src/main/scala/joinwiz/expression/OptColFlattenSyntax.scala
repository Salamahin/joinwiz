package joinwiz.expression

import joinwiz.{LTCol, RTCol}
import org.apache.spark.sql.Column

trait OptColFlattenSyntax {
  implicit class LeftOptColOfOptValueCanFlatten[L, R, T](col: LTCol[L, R, Option[Option[T]]]) {
    def flatten: LTCol[L, R, Option[T]] = new LTCol[L, R, Option[T]] {
      override def column: Column = col.column
      override def apply(value: L): Option[T] = col(value).flatten
    }
  }

  implicit class RightOptColOfOptValueCanFlatten[L, R, T](col: RTCol[L, R, Option[Option[T]]]) {
    def flatten: RTCol[L, R, Option[T]] = new RTCol[L, R, Option[T]] {
      override def column: Column = col.column
      override def apply(value: R): Option[T] = col(value).flatten
    }
  }
}
