package joinwiz.api

import joinwiz.expression.FilterCondition

trait Filter[F[_]] {
  def apply[T](ft: F[T])(predicate: T => Boolean): F[T]

  def byColumn[T](ft: F[T])(cond: FilterCondition[T]): F[T]
}
