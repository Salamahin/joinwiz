package joinwiz.expression

import joinwiz.syntax.{IdLTColMapSyntax, IdRTColMapSyntax, LTColExprSyntax, RTColExprSyntax}
import joinwiz.{Expr, LTCol, RTCol}

trait IsEmptySyntax {
  implicit class LTColOptionIsEmptySyntax[L, R, T](lt: LTCol.Aux[L, R, Option[T]]) {
    def isEmpty: Expr[L, R]   = lt.map(_.isNull, _.isEmpty).expr
    def isDefined: Expr[L, R] = lt.map(_.isNotNull, _.isDefined).expr
  }

  implicit class RTColOptionIsEmptySyntax[L, R, T](rt: RTCol.Aux[L, R, Option[T]]) {
    def isEmpty: Expr[L, R]   = rt.map(_.isNull, _.isEmpty).expr
    def isDefined: Expr[L, R] = rt.map(_.isNotNull, _.isDefined).expr
  }
}
