package joinwiz

object left {
  def unapply[O, A, B](
    left: ApplyToLeftColumn[O, (A, B)]
  ): Option[(ApplyToLeftColumn[O, A], ApplyToLeftColumn[O, B])] = {
    val aExtr = new ApplyToLeftColumn[O, A](left.prefixes :+ "_1", left.extractor.andThen(_._1))
    val bExtr = new ApplyToLeftColumn[O, B](left.prefixes :+ "_2", left.extractor.andThen(_._2))

    Some(aExtr, bExtr)
  }
}

object right {
  def unapply[O, A, B](
    right: ApplyToRightColumn[O, (A, B)]
  ): Option[(ApplyToRightColumn[O, A], ApplyToRightColumn[O, B])] = {
    val aExtr = new ApplyToRightColumn[O, A](right.prefixes :+ "_1", right.extractor.andThen(_._1))
    val bExtr = new ApplyToRightColumn[O, B](right.prefixes :+ "_2", right.extractor.andThen(_._2))

    Some(aExtr, bExtr)
  }
}
