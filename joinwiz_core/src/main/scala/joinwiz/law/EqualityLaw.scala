package joinwiz.law

import joinwiz.{Const, Equality, LTColumn, RTColumn}


trait EqualityLaw[-L, -R] {
  def build(left: L, right: R): Equality
}

trait LowPriorityEqualityLaws {
  def equalityLaw[L, R](func: (L, R) => Equality): EqualityLaw[L, R] = new EqualityLaw[L, R] {
    override def build(left: L, right: R): Equality = func(left, right)
  }

  implicit def equalityIsCommutative[L, R]
  (implicit e: EqualityLaw[R, L]): EqualityLaw[L, R] = equalityLaw {
    (left: L, right: R) => e.build(right, left)
  }
}

trait EqualityLaws extends LowPriorityEqualityLaws {
  implicit def sameTypeCanEqual[LO, RO, T, L, R]: EqualityLaw[LTColumn[LO, L, T], RTColumn[RO, R, T]] = equalityLaw {
    (left: LTColumn[LO, L, T], right: RTColumn[RO, R, T]) => Equality(left, right)
  }

  implicit def rightNullableTypeCanEqual[LO, RO, T, L, R]: EqualityLaw[LTColumn[LO, L, T], RTColumn[RO, R, Option[T]]] = equalityLaw {
    (left: LTColumn[LO, L, T], right: RTColumn[RO, R, Option[T]]) => Equality(left, right)
  }

  implicit def leftNullableTypeCanEqual[LO, RO, T, L, R]: EqualityLaw[LTColumn[LO, L, Option[T]], RTColumn[RO, R, T]] = equalityLaw {
    (left: LTColumn[LO, L, Option[T]], right: RTColumn[RO, R, T]) => Equality(left, right)
  }

  implicit def leftConstCanEqual[LO, T, L]: EqualityLaw[LTColumn[LO, L, T], T] = equalityLaw {
    (col: LTColumn[LO, L, T], const: T) => Equality(col, Const(const))
  }

  implicit def rightConstCanEqual[RO, T, R]: EqualityLaw[RTColumn[RO, R, T], T] = equalityLaw {
    (right: RTColumn[RO, R, T], const: T) => Equality(right, Const(const))
  }

  implicit class EqualitySyntax[L](left: L) {
    def =:=[R](right: R)(implicit eq: EqualityLaw[L, R]): Equality = eq.build(left, right)
  }

}