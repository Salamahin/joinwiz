package joinwiz

import joinwiz.JoinWiz.JOIN_CONDITION
import joinwiz.ops.Join

import scala.language.higherKinds


trait DSLike[F[_]] {

  implicit def joinInstance[T]: Join[F, T]

  implicit def mapInstance[T]: ops.Map[F, T]
}

object DSLike {

  private class DSLikeSyntax[F[_] : DSLike, T](ft: F[T]) {
    def innerJoin[U](fu: F[U])
                    (expr: JOIN_CONDITION[T, U]): DSLikeSyntax[F, (T, U)] = {
      new DSLikeSyntax(implicitly[DSLike[F]].joinInstance.innerJoin(ft, fu)(expr))
    }

    def leftJoin[U](fu: F[U])
                   (expr: JOIN_CONDITION[T, U]): DSLikeSyntax[F, (T, U)] = {
      new DSLikeSyntax(implicitly[DSLike[F]].joinInstance.leftJoin(ft, fu)(expr))
    }

    def map[U <: Product : reflect.runtime.universe.TypeTag](func: T => U): DSLikeSyntax[F, U] = {
      new DSLikeSyntax(implicitly[DSLike[F]].mapInstance.map(ft)(func))
    }

    def unwrap: F[T] = ft
  }

  def wrap[F[_] : DSLike, T, U](ft: F[T]) = new DSLikeSyntax(ft)
}