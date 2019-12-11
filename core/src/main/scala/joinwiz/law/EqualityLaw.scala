package joinwiz.law

import joinwiz.JoinWiz.{LEFT_DS_ALIAS, RIGHT_DS_ALIAS}
import joinwiz._

trait EqualityLaw[T, S] {
  def build(left: T, right: S): Equality
}

trait LowPriorityEqualityLaws {
  def equalityRule[T, S](func: (T, S) => Equality): EqualityLaw[T, S] = new EqualityLaw[T, S] {
    override def build(left: T, right: S): Equality = func(left, right)
  }

  implicit def equalityIsCommutative[T, S]
  (implicit e: EqualityLaw[S, T]): EqualityLaw[T, S] = equalityRule {
    (left: T, right: S) => e.build(right, left)
  }
}

trait EqualityLaws extends LowPriorityEqualityLaws {
  implicit def compatibleTypesAreCommutative[T, S]
  (implicit e: EqualityLaw[LTColumn[S], RTColumn[T]]): EqualityLaw[LTColumn[T], RTColumn[S]] = equalityRule {
    (left: LTColumn[T], right: RTColumn[S]) =>
      e.build(new LTColumn[S](left.name), new RTColumn[T](right.name))
  }

  implicit def sameTypeCanEqual[T]: EqualityLaw[LTColumn[T], RTColumn[T]] = equalityRule {
    (left: LTColumn[T], right: RTColumn[T]) =>
      Equality(
        LeftField(s"$LEFT_DS_ALIAS.${left.name}"), RightField(s"$RIGHT_DS_ALIAS.${right.name}")
      )
  }

  implicit def nullableTypeCanEqual[T]: EqualityLaw[LTColumn[T], RTColumn[Option[T]]] = equalityRule {
    (left: LTColumn[T], right: RTColumn[Option[T]]) =>
      Equality(
        LeftField(s"$LEFT_DS_ALIAS.${left.name}"), RightField(s"$RIGHT_DS_ALIAS.${right.name}")
      )
  }

  implicit def leftConstCanEqual[T]: EqualityLaw[LTColumn[T], T] = equalityRule {
    (left: LTColumn[T], right: T) =>
      Equality(
        LeftField(s"$LEFT_DS_ALIAS.${left.name}"), Const(right)
      )
  }

  implicit def rightConstCanEqual[T]: EqualityLaw[RTColumn[T], T] = equalityRule {
    (left: RTColumn[T], right: T) =>
      Equality(
        LeftField(s"$LEFT_DS_ALIAS.${left.name}"), Const(right)
      )
  }

  implicit class EqualitySyntax[T](left: T) {
    def ===[S](right: S)(implicit eq: EqualityLaw[T, S]): Equality = eq.build(left, right)
  }

}