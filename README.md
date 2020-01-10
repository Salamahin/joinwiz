# joinwiz
Spark API enhancements for Dataset's joins.

**Note the API is evolving and thus may be unstable**


## Motivation
* Prevent from joining by conflicting types like string-decimal
* Specify join condition with lambdas instead of strings
* Opportunity for joining expression analysis for skewed data fixes, statistics collection etc
* Write Dataset transformations and test them in pure-unit style without running spark (testkit)


## Released version

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.github.salamahin/joinwiz_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.github.salamahin/joinwiz_2.11)


## Current version build status
[![Build Status](https://travis-ci.com/Salamahin/joinwiz.svg?branch=master)](https://travis-ci.com/Salamahin/joinwiz)


## Try it
```scala
libraryDependencies += "io.github.salamahin" %% "joinwiz_core" % joinwiz_version
libraryDependencies += "io.github.salamahin" %% "joinwiz_testkit" % joinwiz_version //for testkit
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
    
  import joinwiz.syntax._
    
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
  import joinwiz.testkit.syntax._
    
  ft
    .innerJoin(fu)(
      (l, r) => l(_.pk) =:= r(_.fk) && l(_.value) =:= "val1" && r(_.value) =:= Some(BigDecimal(0L))
    )
    .map {
      case (a, b) => (b, a)
    }
}

//takes millis to run
test("sparkless inner join") { 
  import joinwiz.testkit.sparkless.implicits._
  testMe(as, bs) should contain only ((b1, a1))
}

//takes seconds to run + some spark initialization overhead
test("spark's inner join") {
  import joinwiz.testkit.spark.implicits._
  testMe(aDs, bDs).collect() should contain only ((b1, a1))
}
```