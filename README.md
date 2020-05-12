# joinwiz

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.github.salamahin/joinwiz_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.github.salamahin/joinwiz_2.11) [![Build Status](https://travis-ci.com/Salamahin/joinwiz.svg?branch=master)](https://travis-ci.com/Salamahin/joinwiz)

Spark API enhancements for Dataset's joins.

**Note the API is evolving and thus may be unstable**


## Motivation
* Prevent from joining by conflicting types like string-decimal
* Specify join condition with lambdas instead of strings
* Opportunity for joining expression analysis for skewed data fixes, statistics collection etc
* Write Dataset transformations and test them in pure-unit style without running spark (testkit)




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
    
  case class A(a: String)
  case class B(b: String)
  case class C(c: String)
    
  val aDs: Dataset[A] = ???
  val bDs: Dataset[B] = ???
  val cDs: Dataset[C] = ???
    
  import joinwiz.spark.implicits._
  import joinwiz.syntax._
   
  aDs
    .innerJoin(bDs)((l, r) => l(_.a) =:= r(_.b)) //specify joining columns by types
    .innerJoin(cDs) {
      case (_ joined b, c) => b(_.b) =:= c(_.c)  //or unapply previously joined tuple
    }
    .show()
    
  aDs
    .khomutovJoin(dsB)((left, right) => left(_.a) =:= right(_.b))
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

private def testMe[F[_] : DatasetOperations](fa: F[A], fb: F[B]) = {
  import joinwiz.syntax._
    
  fa
    .innerJoin(fb)(
      (l, r) => l(_.pk) =:= r(_.fk) && l(_.value) =:= "val1" && r(_.value) =:= Some(BigDecimal(0L))
    )
    .map {
      case (a, b) => (b, a)
    }
}

//takes millis to run
test("with sparkless API") { 
  import joinwiz.testkit.implicits._
  testMe(as, bs) should contain only ((b1, a1))
}

//takes seconds to run + some spark initialization overhead
test("with spark API") {
  import joinwiz.spark.implicits._
  testMe(aDs, bDs).collect() should contain only ((b1, a1))
}
```

Supported dataset API
 * inner/left outer joins
 * map
 * flatMap
 * distinct
 * groupByKey + mapGroups
 * filter