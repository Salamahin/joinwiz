package joinwiz.expression

import joinwiz.TColumn
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{udf => sparkUdf}
import scala.reflect.runtime.universe.TypeTag

trait UdfSyntax {
  private def udf[L, R](column: Column)(f: (L, R) => Boolean) = new JoinCondition[L, R] {
    override def apply(): Column                   = column
    override def apply(left: L, right: R): Boolean = f(left, right)
  }

  // format: off
  def udf[L, R, T1: TypeTag](t1: TColumn[L, R, T1])(func: T1 => Boolean): JoinCondition[L, R] = udf(sparkUdf(func).apply(t1.toColumn))((left, right) => func(t1.value(left, right)))
  def udf[L, R, T1: TypeTag, T2: TypeTag](t1: TColumn[L, R, T1],  t2: TColumn[L, R, T2])(func: (T1, T2) => Boolean): JoinCondition[L, R] = udf(sparkUdf(func).apply(t1.toColumn, t2.toColumn))((left, right) => func(t1.value(left, right), t2.value(left, right)))
  def udf[L, R, T1: TypeTag, T2: TypeTag, T3: TypeTag](t1: TColumn[L, R, T1],  t2: TColumn[L, R, T2], t3: TColumn[L, R, T3])(func: (T1, T2, T3) => Boolean): JoinCondition[L, R] = udf(sparkUdf(func).apply(t1.toColumn, t2.toColumn, t3.toColumn))((left, right) => func(t1.value(left, right), t2.value(left, right), t3.value(left, right)))
  def udf[L, R, T1: TypeTag, T2: TypeTag, T3: TypeTag, T4: TypeTag](t1: TColumn[L, R, T1],  t2: TColumn[L, R, T2], t3: TColumn[L, R, T3], t4: TColumn[L, R, T4])(func: (T1, T2, T3, T4) => Boolean): JoinCondition[L, R] = udf(sparkUdf(func).apply(t1.toColumn, t2.toColumn, t3.toColumn, t4.toColumn))((left, right) => func(t1.value(left, right), t2.value(left, right), t3.value(left, right), t4.value(left, right)))
  def udf[L, R, T1: TypeTag, T2: TypeTag, T3: TypeTag, T4: TypeTag, T5: TypeTag](t1: TColumn[L, R, T1],  t2: TColumn[L, R, T2], t3: TColumn[L, R, T3], t4: TColumn[L, R, T4], t5: TColumn[L, R, T5])(func: (T1, T2, T3, T4, T5) => Boolean): JoinCondition[L, R] = udf(sparkUdf(func).apply(t1.toColumn, t2.toColumn, t3.toColumn, t4.toColumn, t5.toColumn))((left, right) => func(t1.value(left, right), t2.value(left, right), t3.value(left, right), t4.value(left, right), t5.value(left, right)))
  // format: on
}
