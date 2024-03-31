package joinwiz.expression

import joinwiz.expression.JoinCondition.joinCondition
import joinwiz.{LTColumn, RTColumn}

trait CanEqual[-A, -B] {
  def apply(a: A, b: B): Boolean
}

trait LowLevelCanEqualDefs {
  def instance[A, B](func: (A, B) => Boolean): CanEqual[A, B] = new CanEqual[A, B] {
    override def apply(a: A, b: B): Boolean = func(a, b)
  }

  implicit def canEqualSameType[A]: CanEqual[A, A] = instance((a, b) => a == b)
}

object CanEqual extends LowLevelCanEqualDefs {
  implicit def canEqualOption[A](implicit ce: CanEqual[A, A]): CanEqual[Option[A], A] = instance((a, b) => a.exists(ce(_, b)))
  implicit def commutativeLaw[A, B](implicit ce: CanEqual[A, B]): CanEqual[B, A]      = instance((a, b) => ce(b, a))
}

trait CanEqualColumn[K, S, L, R] {
  def apply(k: K, s: S): JoinCondition[L, R]
}

trait LowerLevelCanEqualColumn {
  def instance[K, S, L, R](func: (K, S) => JoinCondition[L, R]): CanEqualColumn[K, S, L, R] = new CanEqualColumn[K, S, L, R] {
    override def apply(k: K, s: S): JoinCondition[L, R] = func(k, s)
  }

  implicit def ltEqConst[L, R, T, U](implicit e: CanEqual[T, U]): CanEqualColumn[LTColumn[L, R, T], U, L, R] = instance { (k, s) =>
    joinCondition[L, R]((l, _) => e.apply(k.value(l), s))(k.toColumn === s)
  }

  implicit def rtEqConst[L, R, T, U](implicit e: CanEqual[T, U]): CanEqualColumn[RTColumn[L, R, T], U, L, R] = instance { (k, s) =>
    joinCondition[L, R]((_, r) => e.apply(k.value(r), s))(k.toColumn === s)
  }
}

object CanEqualColumn extends LowerLevelCanEqualColumn {
  implicit def ltEqLt[L, R, T, U](implicit e: CanEqual[T, U]): CanEqualColumn[LTColumn[L, R, T], LTColumn[L, R, U], L, R] = instance { (k, s) =>
    joinCondition[L, R]((l, _) => e.apply(k.value(l), s.value(l)))(k.toColumn === s.toColumn)
  }

  implicit def rtEqRt[L, R, T, U](implicit e: CanEqual[T, U]): CanEqualColumn[RTColumn[L, R, T], RTColumn[L, R, U], L, R] = instance { (k, s) =>
    joinCondition[L, R]((_, r) => e.apply(k.value(r), s.value(r)))(k.toColumn === s.toColumn)
  }

  implicit def ltEqRt[L, R, T, U](implicit e: CanEqual[T, U]): CanEqualColumn[LTColumn[L, R, T], RTColumn[L, R, U], L, R] = instance { (k, s) =>
    joinCondition[L, R]((l, r) => e.apply(k.value(l), s.value(r)))(k.toColumn === s.toColumn)
  }

  implicit def rtEqLt[L, R, T, U](implicit e: CanEqual[T, U]): CanEqualColumn[RTColumn[L, R, T], LTColumn[L, R, U], L, R] = instance { (k, s) =>
    joinCondition[L, R]((l, r) => e.apply(k.value(r), s.value(l)))(k.toColumn === s.toColumn)
  }
}

trait EqualSyntax {
  implicit class CanEqualSyntax[K](k: K) {
    def =:=[S, L, R](s: S)(implicit ce: CanEqualColumn[K, S, L, R]): JoinCondition[L, R] = ce(k, s)
  }
}
