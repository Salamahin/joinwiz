package joinwiz

object left {
  def unapply[O, A, B](
    left: LTColumnExtractor[O, (A, B)]
  ): Option[(LTColumnExtractor[O, A], LTColumnExtractor[O, B])] = {
    val aExtr = new LTColumnExtractor[O, A](left.prefixes :+ "_1", left.extractor.andThen(_._1))
    val bExtr = new LTColumnExtractor[O, B](left.prefixes :+ "_2", left.extractor.andThen(_._2))

    Some(aExtr, bExtr)
  }
}

object right {
  def unapply[O, A, B](
    right: RTColumnExtractor[O, (A, B)]
  ): Option[(RTColumnExtractor[O, A], RTColumnExtractor[O, B])] = {
    val aExtr = new RTColumnExtractor[O, A](right.prefixes :+ "_1", right.extractor.andThen(_._1))
    val bExtr = new RTColumnExtractor[O, B](right.prefixes :+ "_2", right.extractor.andThen(_._2))

    Some(aExtr, bExtr)
  }
}
