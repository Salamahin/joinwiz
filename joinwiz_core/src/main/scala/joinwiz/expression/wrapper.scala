package joinwiz.expression

import joinwiz.{LTCol, RTCol}

final case class LTColOptW[L, R, T] private[joinwiz] (wrapped: LTCol.Aux[L, R, Option[T]])
final case class RTColOptW[L, R, T] private[joinwiz] (wrapped: RTCol.Aux[L, R, Option[T]])

trait Wrappers {
  implicit def ltOptColToWrapper[L, R, T](col: LTCol.Aux[L, R, Option[T]]): LTColOptW[L, R, T] = LTColOptW[L, R, T](col)
  implicit def rtOptColToWrapper[L, R, T](col: RTCol.Aux[L, R, Option[T]]): RTColOptW[L, R, T] = RTColOptW[L, R, T](col)
}
