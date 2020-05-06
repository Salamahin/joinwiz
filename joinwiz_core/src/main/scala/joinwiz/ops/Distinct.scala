package joinwiz.ops

import scala.language.higherKinds

trait Distinct[F[_], T] {
  def apply(ft: F[T]): F[T]
}
