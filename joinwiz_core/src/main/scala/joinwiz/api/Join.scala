package joinwiz.api

import joinwiz.syntax.JOIN_CONDITION

import scala.reflect.runtime.universe.TypeTag

trait Join[F[_]] {
  def inner[LEFT, RIGHT](ft: F[LEFT], fu: F[RIGHT])(expr: JOIN_CONDITION[LEFT, RIGHT]): F[(LEFT, RIGHT)]

  def left[LEFT: TypeTag, RIGHT: TypeTag](ft: F[LEFT], fu: F[RIGHT])(expr: JOIN_CONDITION[LEFT, RIGHT]): F[(LEFT, Option[RIGHT])]

  def left_anti[LEFT: TypeTag, RIGHT](ft: F[LEFT], fu: F[RIGHT])(expr: JOIN_CONDITION[LEFT, RIGHT]): F[LEFT]
}
