package joinwiz

object Un {
  def unapply[O, A, B](left: LTColumnExtractor[O, (A, B)]): Option[(LTColumnExtractor[O, A], LTColumnExtractor[O, B])] = {
    Some(new LTColumnExtractor[O, A](left.prefix :+ "_1", left.extractor.andThen(_._1)), new LTColumnExtractor[O, B](left.prefix :+ "_2",  left.extractor.andThen(_._2)))
  }

  //  def unapply[A, B](left: RTColumnExtractor[(A, B)]): Option[(RTColumnExtractor[A], RTColumnExtractor[B])] = {
  //    Some(new RTColumnExtractor[A](left.level + 1), new RTColumnExtractor[B](left.level + 1))
  //  }
}
