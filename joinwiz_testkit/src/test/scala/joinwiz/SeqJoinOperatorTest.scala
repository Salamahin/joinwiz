package joinwiz

import joinwiz.law.AllLaws
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SeqJoinOperatorTest extends AnyFunSuite with Matchers with AllLaws {

  case class A(pk: String, value: String)

  case class B(fk: String, value: Option[BigDecimal])

  test("inner join") {
    val a1 = A("pk1", "val1")
    val a2 = A("pk1", "val2")
    val a3 = A("pk2", "val3")

    val b1 = B("pk1", Some(BigDecimal(0)))
    val b2 = B("pk2", None)

    import JoinWizLike._
    val joined = implicitly[JoinWizLike[Seq, A, B]].innerJoin(Seq(a1, a2, a3), Seq(b1, b2))((l, r) =>
      l(_.pk) =:= r(_.fk) &&
        l(_.value) =:= "val1" &&
        r(_.value) =:= Some(BigDecimal(0L))
    )

    joined should contain only(
      (a1, b1)
    )
  }
}
