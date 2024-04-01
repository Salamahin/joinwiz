package joinwiz.expression

import joinwiz.expression.JoinCondition.joinCondition
import joinwiz.{LTColumn, RTColumn}

trait CanEqual[-A, -B] {
  def apply(a: A, b: B): Boolean
}

object CanEqual {
  private def instance[A, B](func: (A, B) => Boolean): CanEqual[A, B] = new CanEqual[A, B] {
    override def apply(a: A, b: B): Boolean = func(a, b)
  }

  implicit def canEqualOptionToScalar[A]: CanEqual[Option[A], A]  = instance((a, b) => a contains b)
  implicit def canEqualScalarToOption[A]: CanEqual[A, Option[A]]  = instance((a, b) => b contains a)
  implicit def canEqualSameType[A]: CanEqual[A, A]                = instance((a, b) => a == b)
  implicit def canEqualOptions[A]: CanEqual[Option[A], Option[A]] = instance((a, b) => a.exists(b.contains))
}

trait CanEqualColumn[LARG, RARG, L, R] {
  def apply(larg: LARG, rarg: RARG): JoinCondition[L, R]
}

trait LowerLevelCanEqualColumn {
  def instance[LARG, RARG, L, R](func: (LARG, RARG) => JoinCondition[L, R]): CanEqualColumn[LARG, RARG, L, R] = new CanEqualColumn[LARG, RARG, L, R] {
    override def apply(larg: LARG, rarg: RARG): JoinCondition[L, R] = func(larg, rarg)
  }

  implicit def ltEqConst[L, R, T, U](implicit e: CanEqual[T, U]): CanEqualColumn[LTColumn[L, R, T], U, L, R] = instance { (larg, rarg) =>
    joinCondition[L, R]((l, _) => e(larg.value(l), rarg))(larg.toColumn === rarg)
  }

  implicit def rtEqConst[L, R, T, U](implicit e: CanEqual[T, U]): CanEqualColumn[RTColumn[L, R, T], U, L, R] = instance { (larg, rarg) =>
    joinCondition[L, R]((_, r) => e(larg.value(r), rarg))(larg.toColumn === rarg)
  }
}

object CanEqualColumn extends LowerLevelCanEqualColumn {
  implicit def ltEqLt[L, R, T, U](implicit e: CanEqual[T, U]): CanEqualColumn[LTColumn[L, R, T], LTColumn[L, R, U], L, R] = instance { (k, s) =>
    joinCondition[L, R]((l, _) => e(k.value(l), s.value(l)))(k.toColumn === s.toColumn)
  }

  implicit def rtEqRt[L, R, T, U](implicit e: CanEqual[T, U]): CanEqualColumn[RTColumn[L, R, T], RTColumn[L, R, U], L, R] = instance { (k, s) =>
    joinCondition[L, R]((_, r) => e(k.value(r), s.value(r)))(k.toColumn === s.toColumn)
  }

  implicit def ltEqRt[L, R, T, U](implicit e: CanEqual[T, U]): CanEqualColumn[LTColumn[L, R, T], RTColumn[L, R, U], L, R] = instance { (k, s) =>
    joinCondition[L, R]((l, r) => e(k.value(l), s.value(r)))(k.toColumn === s.toColumn)
  }

  implicit def rtEqLt[L, R, T, U](implicit e: CanEqual[T, U]): CanEqualColumn[RTColumn[L, R, T], LTColumn[L, R, U], L, R] = instance { (k, s) =>
    joinCondition[L, R]((l, r) => e(k.value(r), s.value(l)))(k.toColumn === s.toColumn)
  }
}

trait EqualSyntax {
  implicit class CanEqualSyntax[K](k: K) {
    def =:=[S, L, R](s: S)(implicit ce: CanEqualColumn[K, S, L, R]): JoinCondition[L, R] = ce(k, s)
  }
}
