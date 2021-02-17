package joinwiz.api

import joinwiz.syntax.WINDOW_EXPRESSION
import scala.reflect.runtime.universe.TypeTag

trait WithWindow[F[_], T] {
  def apply[S](fo: F[T])(withWindow: WINDOW_EXPRESSION[T, S])(implicit tt: TypeTag[(T, S)]): F[(T, S)]
}
