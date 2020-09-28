package joinwiz.expression

import joinwiz.{LTCol, RTCol}

final case class LTColW[L, R, T] private[joinwiz] (wrapped: LTCol[L, R, T])
final case class RTColW[L, R, T] private[joinwiz] (wrapped: RTCol[L, R, T])
final case class LTColOptW[L, R, T] private[joinwiz] (wrapped: LTCol[L, R, Option[T]])
final case class RTColOptW[L, R, T] private[joinwiz] (wrapped: RTCol[L, R, Option[T]])

trait Wrappers {
  implicit def ltColToWrapper[L, R, T](col: LTCol[L, R, T]): LTColW[L, R, T]               = LTColW[L, R, T](col)
  implicit def rtColToWrapper[L, R, T](col: RTCol[L, R, T]): RTColW[L, R, T]               = RTColW[L, R, T](col)
  implicit def ltOptColToWrapper[L, R, T](col: LTCol[L, R, Option[T]]): LTColOptW[L, R, T] = LTColOptW[L, R, T](col)
  implicit def rtOptColToWrapper[L, R, T](col: RTCol[L, R, Option[T]]): RTColOptW[L, R, T] = RTColOptW[L, R, T](col)
}
