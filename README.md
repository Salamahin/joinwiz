# joinwiz
Spark API enhancements for Dataset's joins.

## Motivation
* Prevent from joining by conflicting types like string-decimal
* Specify join condition with lambdas instead of strings
* Opportunity for joining expression analysis for skewed data fixes, statistics collection etc

## Try it
```scala
libraryDependencies += "io.github.salamahin" %% "joinwiz_core" % "0.1.0"
```

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
  val dsB = Seq(B(Some("pk1"))).toDS()I
    
  import JoinWiz._
    
  dsA
    .innerJoin(dsB)((left, right) => left(_.pk) =:= right(_.fk))
    .show()
    
  dsA
    .khomutovJoin(dsB)((left, right) => left(_.pk) =:= right(_.fk))
    .show()
}
```

## Testkit

Running spark tests takes ages. Sometimes it worthful to test something relatively simple like set of joins and maps
without actually running spark.
With testkit you can write something like:
```scala
val as = Seq(a1, a2, a3)
val bs = Seq(b1, b2)
val aDs = as.toDS()
val bDs = bs.toDS()


private def testMe[F[_] : DatasetOperations](ft: F[A], fu: F[B]) = {
  import joinwiz.testkit._
    
  ft
    .innerJoin(fu)(
      (l, r) => l(_.pk) =:= r(_.fk) && l(_.value) =:= "val1" && r(_.value) =:= Some(BigDecimal(0L))
    )
    .map {
      case (a, b) => (b, a)
    }
}

test("sparkless inner join") {
  import joinwiz.testkit.sparkless._
  testMe(as, bs) should contain only ((b1, a1))
}


test("spark's inner join") {
  import joinwiz.testkit.spark._
  testMe(aDs, bDs).collect() should contain only ((b1, a1))
}
```