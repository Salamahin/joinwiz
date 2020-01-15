package joinwiz.ops

import scala.language.higherKinds

trait Filter[F[_], T] {
  def apply(ft: F[T])(predicate: T => Boolean): F[T]
}
