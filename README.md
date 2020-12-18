# joinwiz

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.github.salamahin/joinwiz_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.github.salamahin/joinwiz_2.11) [![Build Status](https://travis-ci.com/Salamahin/joinwiz.svg?branch=master)](https://travis-ci.com/Salamahin/joinwiz)

Spark API enhancements for Dataset's joins.
 * Typesafe - checks the type of the joining expression in compile time
 * Run (some) unit tests without SparkSession
 * No reflection

**Note the API is evolving and thus may be unstable**

## Try it
```scala
libraryDependencies += "io.github.salamahin" %% "joinwiz_core" % joinwiz_version
libraryDependencies += "io.github.salamahin" %% "joinwiz_testkit" % joinwiz_version //for testkit
```


## Primitive join
```scala
def doJoin[F[_]: ComputationEngine](as: F[A], bs: F[B]): F[(A, Option[B])] = {
  import joinwiz.syntax._
  as.leftJoin(bs) {
    case (left, right) => left(_.field) =:= right(_.field)
  }
}
```

Injecting an `ComputationEngine` allows to make an abstraction over exact kind, which means it's possible to run
the code in 2 modes: with and without spark
```scala
import jointwiz.spark._
val as: Dataset[A] = ???
val bs: Dataset[B] = ???
doJoin(as, bs) //will run using SparkSession and result is a Dataset
```

On the other hand for test purposes you can do the following
```scala
import jointwiz.testkit._
val as: Seq[A] = ???
val bs: Seq[B] = ???
doJoin(as, bs) //will run without SparkSession and result is a Seq
```

Clearly testing without spark makes your unit-test run much faster 

## Chained joins
In case when several joins are made one-by-one it might be tricky to check which exactly col in which table is used.
You can easily workaround with unapplication
```scala
def doSequentialJoin[F[_]: ComputationEngine](as: F[A], bs: F[B], cs: F[C]) = {
  import joinwiz.syntax._
  as
    .leftJoin(bs) {
      case (left, right) => left(_.field) =:= right(_.field)
    }
    .leftJoin(cs) {
      case (_ wiz b, c) => b(_.field) =:= c(_.field)
    }
}
```

Unapply can be used to extract a members from a product type even if the type of option kind

## UDFs
Mapping of the joining expression is supported. To make the changes usable in testkit, one must specify transformation
implementation on both `Column` and type
```scala
def joinWithMap[F[_]: ComputationEngine](as: F[A], bs: F[B]) = {
  import joinwiz.syntax._
  as
    .leftJoin(bs) {
      case (left, right) => left(_.field).map(column => column.cast(StringType), value => value.toString) =:= right(_.field)
    }
}
```

## Behind joins
`ComputationEngine` provides syntax for generic operations like:
  * inner/left outer joins
  * map
  * flatMap
  * distinct
  * groupByKey + mapGroups, reduceGroups, count, cogroup
  * filter
  * collect