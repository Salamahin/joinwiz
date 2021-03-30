package joinwiz.api

import joinwiz.syntax.WINDOW_EXPRESSION
import scala.reflect.runtime.universe.TypeTag

trait WithWindow[F[_]] {
  def apply[T: TypeTag, S: TypeTag](fo: F[T])(withWindow: WINDOW_EXPRESSION[T, S]): F[(T, S)]
}
