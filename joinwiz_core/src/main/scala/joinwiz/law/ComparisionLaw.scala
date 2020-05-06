package joinwiz.law

trait ComparisionLaw[L, R] {
  def build(left: L, right: R): And
}
