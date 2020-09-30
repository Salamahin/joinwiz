package joinwiz.expression

import joinwiz.syntax.{IdLTColMapSyntax, IdRTColMapSyntax, LTColExprSyntax, RTColExprSyntax}
import joinwiz.{Expr, LTCol, RTCol}

trait IsEmptySyntax {
  implicit class LTColOptionIsEmptySyntax[L, R, T](lt: LTCol[L, R, Option[T]]) {
    def isEmpty: Expr[L, R]   = lt.map(_.isEmpty)(_.isNull).expr
    def isDefined: Expr[L, R] = lt.map(_.isDefined)(_.isNotNull).expr
  }

  implicit class RTColOptionIsEmptySyntax[L, R, T](rt: RTCol[L, R, Option[T]]) {
    def isEmpty: Expr[L, R]   = rt.map(_.isEmpty)(_.isNull).expr
    def isDefined: Expr[L, R] = rt.map(_.isDefined)(_.isNotNull).expr
  }
}
