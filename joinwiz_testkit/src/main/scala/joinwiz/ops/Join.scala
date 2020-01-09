package joinwiz.ops

import joinwiz.JoinWiz.JOIN_CONDITION

trait Join[F[_], T] {
  def innerJoin[U](ft: F[T], fu: F[U])(expr: JOIN_CONDITION[T, U]): F[(T, U)]

  def leftJoin[U](ft: F[T], fu: F[U])(expr: JOIN_CONDITION[T, U]): F[(T, U)]
}
