package joinwiz.testkit.template

import java.sql.{Date, Timestamp}

import shapeless.{::, Generic, HList, HNil}

trait Filler[Out] {
  def fill(): Out
}


trait ZeroTemplates {
  self: InstanceCreator =>

  implicit def optTemplate[T]: Filler[Option[T]] = fillerInstance[Option[T]](None)

  implicit def seqTemplate[T]: Filler[Seq[T]] = fillerInstance[Seq[T]](Nil)

  implicit val strTemplate: Filler[String] = fillerInstance[String]("")

  implicit val intTemplate: Filler[Int] = fillerInstance[Int](0)

  implicit val longTemplate: Filler[Long] = fillerInstance[Long](0L)

  implicit val dateTemplate: Filler[Date] = fillerInstance[Date](new Date(0L))

  implicit val tsTemplate: Filler[Timestamp] = fillerInstance[Timestamp](new Timestamp(0L))

  implicit val decTemplate: Filler[BigDecimal] = fillerInstance[BigDecimal](BigDecimal(0L))

  implicit val boolTemplate: Filler[Boolean] = fillerInstance[Boolean](false)
}

class InstanceCreator {

  def fillerInstance[A](value: => A): Filler[A] = new Filler[A] {
    override def fill(): A = value
  }

  implicit val hnilTemplate: Filler[HNil] = fillerInstance[HNil](HNil)

  implicit def hConsTemplate[H, T <: HList]
  (implicit
   hFiller: Filler[H],
   tFiller: Filler[T]
  ): Filler[H :: T] = new Filler[H :: T] {
    override def fill(): H :: T = hFiller.fill() :: tFiller.fill()
  }

  implicit def genericTemplate[P, PL <: HList]
  (implicit
   gen: Generic.Aux[P, PL],
   filler: Filler[PL]
  ): Filler[P] = new Filler[P] {
    override def fill(): P = gen.from(filler.fill())
  }

  def apply[P](implicit s: Filler[P]): P = s.fill()
}
