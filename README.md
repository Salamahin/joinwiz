# joinwiz
Spark API enhancements for Dataset's joins.

## Motivation
* Prevent from joining by conflicting types like string-decimal
* Specify join condition with lambdas instead of strings
* Gives opportunity for joining expression analysis for skewed data fixes, statistics collection etc

## Usage

```scala
case class A(pk: String)
case class B(fk: Option[String])

val dsA: Dataset[A] = ???
val dsB: Dataset[B] = ???

import JoinWiz._
dsA.innerJoin(dsB)((left, right) => left(_.pk) =:= right(_.fk))
```