package joinwiz.api

import joinwiz.syntax.JOIN_CONDITION

trait Join[F[_], T] {
  def inner[U](ft: F[T], fu: F[U])(expr: JOIN_CONDITION[T, U]): F[(T, U)]

  def left[U](ft: F[T], fu: F[U])(expr: JOIN_CONDITION[T, U]): F[(T, U)]
}
