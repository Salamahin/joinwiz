package joinwiz.law

import joinwiz.{Const, Equality, LTColumn, RTColumn}


trait EqualityLaw[-T, -S] {
  def build(left: T, right: S): Equality
}

trait LowPriorityEqualityLaws {
  def equalityLaw[T, S](func: (T, S) => Equality): EqualityLaw[T, S] = new EqualityLaw[T, S] {
    override def build(left: T, right: S): Equality = func(left, right)
  }

  implicit def equalityIsCommutative[T, S]
  (implicit e: EqualityLaw[S, T]): EqualityLaw[T, S] = equalityLaw {
    (left: T, right: S) => e.build(right, left)
  }
}

trait EqualityLaws extends LowPriorityEqualityLaws {
  implicit def sameTypeCanEqual[T, U, V]: EqualityLaw[LTColumn[U, T], RTColumn[V, T]] = equalityLaw {
    (left: LTColumn[U, T], right: RTColumn[V, T]) => Equality(left, right)
  }

  implicit def rightNullableTypeCanEqual[T, U, V]: EqualityLaw[LTColumn[U, T], RTColumn[V, Option[T]]] = equalityLaw {
    (left: LTColumn[_, T], right: RTColumn[_, Option[T]]) => Equality(left, right)
  }

  implicit def leftNullableTypeCanEqual[T, U, V]: EqualityLaw[LTColumn[U, Option[T]], RTColumn[V, T]] = equalityLaw {
    (left: LTColumn[_, Option[T]], right: RTColumn[_, T]) => Equality(left, right)
  }

  implicit def leftConstCanEqual[T, U]: EqualityLaw[LTColumn[U, T], T] = equalityLaw {
    (col: LTColumn[U, T], const: T) => Equality(col, Const(const))
  }

  implicit def rightConstCanEqual[T, V]: EqualityLaw[RTColumn[V, T], T] = equalityLaw {
    (right: RTColumn[V, T], const: T) => Equality(right, Const(const))
  }

  implicit class EqualitySyntax[T](left: T) {
    def =:=[S](right: S)(implicit eq: EqualityLaw[T, S]): Equality = eq.build(left, right)
  }

}