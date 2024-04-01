package joinwiz.expression

import joinwiz.expression.JoinCondition.joinCondition
import joinwiz.{LTColumn, RTColumn}

trait CanEqualColumn[-LARG, -RARG, L, R] {
  def apply(larg: LARG, rarg: RARG): JoinCondition[L, R]
}

trait LowerLevelCanEqualColumn {
  def instance[LARG, RARG, L, R](func: (LARG, RARG) => JoinCondition[L, R]): CanEqualColumn[LARG, RARG, L, R] = new CanEqualColumn[LARG, RARG, L, R] {
    override def apply(larg: LARG, rarg: RARG): JoinCondition[L, R] = func(larg, rarg)
  }

  implicit def ltEqConst[L, R, T]: CanEqualColumn[LTColumn[L, R, T], T, L, R] = instance { (larg, rarg) => joinCondition[L, R]((l, _) => larg.value(l) == rarg)(larg.toColumn === rarg) }
  implicit def rtEqConst[L, R, T]: CanEqualColumn[RTColumn[L, R, T], T, L, R] = instance { (larg, rarg) => joinCondition[L, R]((_, r) => larg.value(r) == rarg)(larg.toColumn === rarg) }
}

object CanEqualColumn extends LowerLevelCanEqualColumn {
  implicit def ltTEqRtT[L, R, T]: CanEqualColumn[LTColumn[L, R, T], RTColumn[L, R, T], L, R] = instance { (larg, rarg) =>
    joinCondition[L, R]((l, r) => larg.value(l) == rarg.value(r))(larg.toColumn === rarg.toColumn)
  }

  implicit def ltOptTEqOptT[L, R, T]: CanEqualColumn[LTColumn[L, R, Option[T]], Option[T], L, R] = instance { (larg, rarg) =>
    joinCondition[L, R]((l, _) => larg.value(l).exists(rarg.contains))(if (rarg.isDefined) larg.toColumn === rarg.get else larg.toColumn.isNull)
  }

  implicit def ltOptTEqRtT[L, R, T]: CanEqualColumn[LTColumn[L, R, Option[T]], RTColumn[L, R, T], L, R] = instance { (larg, rarg) =>
    joinCondition[L, R]((l, r) => larg.value(l) contains rarg.value(r))(larg.toColumn === rarg.toColumn)
  }

  implicit def ltTEqRtOptT[L, R, T]: CanEqualColumn[LTColumn[L, R, T], RTColumn[L, R, Option[T]], L, R] = instance { (larg, rarg) =>
    joinCondition[L, R]((l, r) => rarg.value(r) contains larg.value(l))(larg.toColumn === rarg.toColumn)
  }

  implicit def ltOptTEqRtOptT[L, R, T]: CanEqualColumn[LTColumn[L, R, Option[T]], RTColumn[L, R, Option[T]], L, R] = instance { (larg, rarg) =>
    joinCondition[L, R]((l, r) =>
      (for {
        ll <- larg.value(l)
        rr <- rarg.value(r)
      } yield ll == rr).getOrElse(false)
    )(larg.toColumn === rarg.toColumn)
  }
}

trait EqualSyntax {
  implicit class CanEqualSyntax[K](k: K) {
    def =:=[S, L, R](s: S)(implicit ce: CanEqualColumn[K, S, L, R]): JoinCondition[L, R] = ce(k, s)
  }
}
