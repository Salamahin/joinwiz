# joinwiz

[![Build Status](https://travis-ci.com/Salamahin/joinwiz.svg?branch=master)](https://travis-ci.com/Salamahin/joinwiz)

Tiny library improves Spark's dataset join API and improves unit-testing experience of (some) Spark transformations

## Why
There are 2 main reasons - using typesafe Dataset API one still need to specify the joining condition with strings or
expressions which is not convenient and possible can be reason of a silly mistake. On the other hand with the power of
macroses one can extract fields are used in the expression in the same manner as it is implemented in various lens libs.
That will let your IDE to help you build an expression and will prevent from comparing incompatible types (like string-
decimal join when spark casts both left and right values to double)

The second reason is that unit testing with Spark is a nightmare. It takes seconds for local session to start which 
means you will be running your single suite for a minute or two. On the other hand Scala has an abstraction over type - 
higher kinds. Most popular spark transformations can be expressed on top of Datasets and any Seq, and `joinwiz-testkit` 
allows you to do so **without even creating a Spark context**, and that will your tests super fast. Of course not every 
transformation has an analogue in Seq's terms (like `repartition` makes sence only for distributed collections) but
such specific behaviour still can be isolated easily.

## Try it
```scala
libraryDependencies += "io.github.salamahin" %% "joinwiz_core" % joinwiz_version
```


## Primitive join
Note that result has type of `(A, Option[B])` - no more NPE's when mapping!
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
You can easily workaround this with `wiz` unapplication
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

## Window functions
To add a new window function one has to inherit `joinwiz.window.WindowFunction`. After this can be used like following:
```scala
def addRowNumber[F[_]: ComputationEngine](as: F[A]): F[(A, Int)] = {
  import joinwiz.syntax._
  as.withWindow { window =>
    window
      .partitionBy(_.field1)
      .partitionBy(_.field2)
      .orderByAsc(_.field3)
      .call(row_number)
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

You can find more examples of usage in appropriate [test](joinwiz_core/src/test/scala/joinwiz/ComputationEngineTest.scala)