package joinwiz.api

import joinwiz.syntax.JOIN_CONDITION

import scala.reflect.runtime.universe.TypeTag

trait Join[F[_], T] {
  def inner[U](ft: F[T], fu: F[U])(expr: JOIN_CONDITION[T, U]): F[(T, U)]

  def left[U](ft: F[T], fu: F[U])(expr: JOIN_CONDITION[T, U])(implicit tt: TypeTag[(T, Option[U])]): F[(T, Option[U])]
}
