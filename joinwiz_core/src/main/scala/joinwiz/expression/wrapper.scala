package joinwiz.expression

import joinwiz.{LeftColumn, RightColumn}

final case class LeftColumnOptW[L, R, T] private[joinwiz] (wrapped: LeftColumn[L, R, Option[T]])
final case class RightColumnOptW[L, R, T] private[joinwiz] (wrapped: RightColumn[L, R, Option[T]])

trait Wrappers {
  implicit def ltOptColToWrapper[L, R, T](col: LeftColumn[L, R, Option[T]]): LeftColumnOptW[L, R, T] = LeftColumnOptW[L, R, T](col)
  implicit def rtOptColToWrapper[L, R, T](col: RightColumn[L, R, Option[T]]): RightColumnOptW[L, R, T] = RightColumnOptW[L, R, T](col)
}
