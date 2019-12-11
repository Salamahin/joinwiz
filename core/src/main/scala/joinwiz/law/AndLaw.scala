package joinwiz.law

import joinwiz.{And, Operator}

trait AndLaw[T, S] {
  def build(left: T, right: S): And
}

trait AndLaws {
  implicit def canComposeOperators[T <: Operator, S <: Operator]: AndLaw[T, S] = new AndLaw[T, S] {
    override def build(left: T, right: S): And = And(left, right)
  }

  implicit class AndSyntax[T](left: T) {
    def &&[S](right: S)(implicit and: AndLaw[T, S]): And = and.build(left, right)
  }
}
