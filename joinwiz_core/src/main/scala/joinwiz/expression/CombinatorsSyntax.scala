package joinwiz.expression

import JoinCondition._

trait CombinatorsSyntax {
  implicit class CombineExpressionsSyntax[L, R](thisExpr: JoinCondition[L, R]) {
    def &&(thatExpr: JoinCondition[L, R]): JoinCondition[L, R] = joinCondition[L, R]((l, r) => thisExpr(l, r) && thatExpr(l, r))(thisExpr() && thatExpr())
  }
}
