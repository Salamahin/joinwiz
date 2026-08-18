# joinwiz

[![build](https://github.com/Salamahin/joinwiz/actions/workflows/ci.yml/badge.svg)](https://github.com/Salamahin/joinwiz/actions/workflows/ci.yml)

Tiny library improves Spark's dataset join API by allowing you to specify join columns with lambdas instead of strings,
ensuring typesafety and allows you using autocomplete features of your IDE. Also improves unit-testing experience
of (some) Spark transformations

## Version compatibility

| joinwiz artifact | Spark | Scala | Java |
|---|---|---|---|
| `joinwiz_core-spark2` |  2.4.5 | 2.12 | 8+ |
| `joinwiz_core-spark3` | 3.2.1 | 2.13 | 11+ |
| `joinwiz_core-spark4` | 4.1.0 | 2.13 | 17+ |

## Try it

[![joinwiz_core Scala version support](https://index.scala-lang.org/salamahin/joinwiz/joinwiz_core/latest.svg)](https://index.scala-lang.org/salamahin/joinwiz/joinwiz_core)
```scala
scalacOptions += "-Ydelambdafy:inline"
// For Spark 3 with Scala 2.13:
libraryDependencies += "io.github.salamahin" %% "joinwiz_core-spark3" % joinwiz_version
// For Spark 4 with Scala 2.13:
libraryDependencies += "io.github.salamahin" %% "joinwiz_core-spark4" % joinwiz_version
// For Spark 2 with Scala 2.12:
libraryDependencies += "io.github.salamahin" %% "joinwiz_core-spark2" % joinwiz_version
```

## Simple join

```scala
def doJoin(as: Dataset[A], bs: Dataset[B]): Dataset[(A, Option[B])] = {
  import joinwiz.syntax._
  import joinwiz.spark._
  as.leftJoin(bs) {
    case (left, right) => left(_.field) =:= right(_.field)
  }
}

```
Note, that result has a type of `Dataset[(A, Option[B])]` which means you won't get an NPE when would try a map it to a different type.
In addition the library checks if both left and right columns can be used in the joining expression, meaning they need to have
the comparable type.
You are not limited to equal join only, one can use `>`, `<`, `&&`, consts and more


`ComputationEngine` allows to make an abstraction over exact kind, which means it's possible to run the
code in 2 modes: with and without spark:
```scala
def foo[F[_]: ComputationEngine](as: F[A], bs: F[B]): F[C] = {
  import joinwiz.syntax._
  as
    .innerJoin(bs) {
      case (a, b) => a(_.field) =:= b(_.field)
    }
    .map {
      case (a, b) => C(a, b)
    }
}

def runWithSpark(as: Dataset[A], bs: Dataset[B]): Dataset[C] = {
  import joinwiz.spark._
  foo(as, bs)
}

//can be used in unit-testing
def runWithoutSpark(as: Seq[A], bs: Seq[B]): Seq[C] = {
  import joinwiz.testkit._
  foo(as, bs)
}
```

## Chained joins

In case when several joins are made one-by-one it might be tricky to reference the exact column with a string identifier,
usually you would see something like `_1._1._1.field` from left or right side.
With help of `wiz` unapplication you can transform that to a nice lambdas:
```scala
def doSequentialJoin(as: Dataset[A],
                     bs: Dataset[B],
                     cs: Dataset[C],
                     ds: Dataset[D]): Dataset[(((A, Option[B]), Option[C]), Option[D])] = {
  import joinwiz.syntax._
  import joinwiz.spark._
  as
    .leftJoin(bs) {
      case (a, b) => a(_.field) =:= b(_.field)
    }
    .leftJoin(cs) {
      case (_ wiz b, c) => b(_.field) =:= c(_.field)
    }
    .leftJoin(ds) {
      case (_ wiz _ wiz c, d) => c(_.field) =:= d(_.field)
    }
}
```
Unapply can be used to extract a members from a product type even if the type of option kind

## Nested structures

Assuming your case-class contains some nested structs, in such case you can still can use joinwiz to extract necessary column:
```scala
def doJoin[F[_]: ComputationEngine](as: F[A], bs: F[B]): F[(A, Option[B])] = {
  import joinwiz.syntax._
  as
    .leftJoin(bs) {
      case (left, right) => left >> (_.innerStruct) >> (_.field) =:= bs >> (_.field)
    }
}
```

Operation `>>` is an alias for `apply`

## UDFs

One can use UDF as a joining expressions

```scala
def doJoin[F[_] : ComputationEngine](as: F[A], bs: F[B]): F[(A, Option[B])] = {
  import joinwiz.syntax._
  as.leftJoin(bs) {
    case (left, right) =>
      udf(
        left(_.field),
        right(_.field)
      )(_ + _ < 3)
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

* inner/left outer/left anti joins
* map
* flatMap
* distinct
* groupByKey + mapGroups, reduceGroups, count, cogroup
* filter
* collect

You can find more examples of usage in the appropriate [test](joinwiz_core/src/test/scala/joinwiz/ComputationEngineTest.scala)

## Testing without Spark

Any transformation written generically over `F[_]: ComputationEngine` can be exercised in a
plain unit test — no `SparkSession`, no serialization, no JVM warm-up — by running it through the
`Seq` interpreter from `joinwiz.testkit`:

```scala
import joinwiz.syntax._

def enrich[F[_]: ComputationEngine](users: F[User], orders: F[Order]): F[(User, Option[Order])] =
  users.leftJoin(orders)((u, o) => u(_.id) =:= o(_.userId))

// production
def enrichSpark(users: Dataset[User], orders: Dataset[Order]) = {
  import joinwiz.spark._
  enrich(users, orders)
}

// test — same code, ordinary Seq, assert on the result directly
def enrichLocally(users: Seq[User], orders: Seq[Order]) = {
  import joinwiz.testkit._
  enrich(users, orders)
}
```

The two interpreters are held to the same behaviour by
[`EngineParitySpec`](joinwiz_core/src/test/scala/joinwiz/EngineParitySpec.scala), a property-based
suite that feeds randomized data through both `Dataset` and `Seq` and asserts the results match.

## Development

The build is parameterized by Spark major version via the `spark.version` system property
(`2`, `3` or `4`), each pinned to its own Scala/JDK combination (see the table above). Use the
matching JDK when running a given line locally.

```bash
sbt -Dspark.version=4 +test          # compile & test against Spark 4 / Scala 2.13 (JDK 17)
sbt -Dspark.version=3 +test          # Spark 3 / Scala 2.13 (JDK 11)
sbt -Dspark.version=2 +test          # Spark 2 / Scala 2.12 (JDK 8)

sbt -Dspark.version=4 scalafmtAll    # apply formatting (checked in CI)
```

Spark itself is a `Provided` dependency, so consuming applications supply `spark-core` /
`spark-sql` from their own runtime; the test suite pulls them in through the test classpath.

