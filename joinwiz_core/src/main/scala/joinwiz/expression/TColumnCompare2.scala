package joinwiz.expression

import joinwiz.Id

trait TColumnCompare2[F[_]] {
  def equals[T](thisFt: F[T], thatT: T): Boolean
  def equals[T](thisFt: F[T], thatOptT: Option[T]): Boolean

  def compare[T: Ordering](thisFt: F[T], thatT: T)(expected: Int*): Boolean
  def compare[T: Ordering](thisFt: F[T], thatT: Option[T])(expected: Int*): Boolean
}

object TColumnCompare2 {
  private def compareEither[T](x: T, y: T)(expected: Int*)(implicit o: Ordering[T]) = {
    val compared = o.compare(x, y)

    expected.foldLeft(false) {
      case (prev, nextExpected) => prev || compared == nextExpected
    }
  }

  private def compareEither[T](x: T, y: Option[T])(expected: Int*)(implicit o: Ordering[T]) = {
    val compared = y.map(o.compare(x, _))

    expected.foldLeft(false) {
      case (prev, nextExpected) => prev || compared.contains(nextExpected)
    }
  }

  private def compareEither[T](x: Option[T], y: Option[T])(expected: Int*)(implicit o: Ordering[T]) = {
    val compared = for {
      xx <- x
      yy <- y
    } yield o.compare(xx, yy)

    expected.foldLeft(false) {
      case (prev, nextExpected) => prev || compared.contains(nextExpected)
    }
  }

  implicit val compareOfOptionTColumn: TColumnCompare2[Option] = new TColumnCompare2[Option] {
    override def equals[T](thisFt: Option[T], thatT: T): Boolean            = thisFt.contains(thatT)
    override def equals[T](thisFt: Option[T], thatOptT: Option[T]): Boolean = thisFt.isDefined && thisFt == thatOptT

    override def compare[T: Ordering](thisFt: Option[T], thatT: T)(expected: Int*): Boolean         = compareEither(thatT, thisFt)(expected.map(-_): _*)
    override def compare[T: Ordering](thisFt: Option[T], thatT: Option[T])(expected: Int*): Boolean = compareEither(thisFt, thatT)(expected: _*)
  }

  implicit val compareOfIdTColumn: TColumnCompare2[Id] = new TColumnCompare2[Id] {
    override def equals[T](thisFt: Id[T], thatT: T): Boolean            = thisFt == thatT
    override def equals[T](thisFt: Id[T], thatOptT: Option[T]): Boolean = thatOptT.contains(thisFt)

    override def compare[T: Ordering](thisFt: Id[T], thatT: T)(expected: Int*): Boolean         = compareEither(thisFt, thatT)(expected: _*)
    override def compare[T: Ordering](thisFt: Id[T], thatT: Option[T])(expected: Int*): Boolean = compareEither(thisFt, thatT)(expected: _*)
  }
}
