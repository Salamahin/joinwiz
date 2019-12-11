# joinwiz
Spark API enhancements for Dataset's joins.

## Motivation
* Prevent from joining by conflicting types like string-decimal
* Specify join condition with lambdas instead of strings
* Gives opportunity for joining expression analysis for skewed data fixes, statistics collection etc

## Usage

```scala
object Runner extends App {
  val ss = SparkSession.builder()
    .master("local[*]")
    .getOrCreate()

  import ss.implicits._

  case class A(pk: String)
  case class B(fk: Option[String])

  val dsA = Seq(A("pk1")).toDS()
  val dsB = Seq(B(Some("pk1"))).toDS()

  dsA
    .innerJoin(dsB)((left, right) => left(_.pk) =:= right(_.fk))
    .show()

  dsA
    .khomutovJoin(dsB)((left, right) => left(_.pk) =:= right(_.fk))
    .show()
}
```