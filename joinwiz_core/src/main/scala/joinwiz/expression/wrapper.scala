package joinwiz.expression

import joinwiz.{LTColumn, RTColumn}

final case class LTColumnOptW[L, R, T] private[joinwiz] (wrapped: LTColumn[L, R, Option[T]])
final case class RTColumnOptW[L, R, T] private[joinwiz] (wrapped: RTColumn[L, R, Option[T]])

trait Wrappers {
  implicit def ltOptColToWrapper[L, R, T](col: LTColumn[L, R, Option[T]]): LTColumnOptW[L, R, T] = LTColumnOptW[L, R, T](col)
  implicit def rtOptColToWrapper[L, R, T](col: RTColumn[L, R, Option[T]]): RTColumnOptW[L, R, T] = RTColumnOptW[L, R, T](col)
}
