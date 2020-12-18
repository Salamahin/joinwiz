package joinwiz.expression

import joinwiz.ApplyLTCol

trait ExtraApplyTColSyntax {
  implicit class UnwrapOptionLTColSyntax[LO, RO, E](lt: ApplyLTCol[LO, RO, Option[E]]) {
    def ? = new ApplyLTCol[LO, RO, Option[E]](lt.names,  lt.orig)
  }
}
