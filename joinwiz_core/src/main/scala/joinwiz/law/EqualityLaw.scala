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


//    (left: LTColumn[T], right: RTColumn[S]) => {
//      val newLeft = new LTColumn[S] {
//        override val name: String = left.name
//        override type Orig = right.Orig
//        override val expr: this.Orig => S = right.expr
//      }
//
//      val newRight = new RTColumn[T] {
//        override val name: String = right.name
//        override type Orig = left.Orig
//        override val expr: this.Orig => T = left.expr
//      }
//
//      e.build(newLeft, newRight)
    }
  }

  implicit def sameTypeCanEqual[T]: EqualityLaw[LTColumn[T], RTColumn[T]] = equalityRule {
    (left: LTColumn[T], right: RTColumn[T]) =>
      Equality(LeftField(left.name), RightField(right.name))
  }

  implicit def nullableTypeCanEqual[T]: EqualityLaw[LTColumn[T], RTColumn[Option[T]]] = equalityRule {
    (left: LTColumn[T], right: RTColumn[Option[T]]) =>
      Equality(LeftField(left.name), RightField(right.name))
  }

  implicit def leftConstCanEqual[T]: EqualityLaw[LTColumn[T], T] = equalityRule {
    (leftCol: LTColumn[T], const: T) =>
      Equality(LeftField(leftCol.name), Const(const))
  }

  implicit def rightConstCanEqual[T]: EqualityLaw[RTColumn[T], T] = equalityRule {
    (rightCol: RTColumn[T], const: T) =>
      Equality(RightField(rightCol.name), Const(const))
  }

  implicit class EqualitySyntax[T](left: T) {
    def =:=[S](right: S)(implicit eq: EqualityLaw[T, S]): Equality = eq.build(left, right)
  }

}