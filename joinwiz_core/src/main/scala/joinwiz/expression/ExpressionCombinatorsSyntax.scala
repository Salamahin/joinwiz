package joinwiz.expression

import joinwiz.Expr
import joinwiz.Expr._

trait ExpressionCombinatorsSyntax {
  implicit class CombineExpressionsSyntax[L, R](thisExpr: Expr[L, R]) {
    def &&(thatExpr: Expr[L, R]): Expr[L, R] = expr[L, R](thisExpr.apply && thatExpr.apply)(
      (l, r) => thisExpr(l, r) && thatExpr(l, r)
    )
  }
}
